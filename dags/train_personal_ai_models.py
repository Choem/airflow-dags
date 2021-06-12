import json
from pathlib import Path
from datetime import timedelta, datetime
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

def get_secret(secret_name):
    secrets_dir = Path('/opt/airflow/secrets')
    secret_path = secrets_dir / secret_name
    assert secret_path.exists(), f'could not find {secret_name} at {secret_path}'
    files = {}
    for entity in Path(secret_path).iterdir():
        if entity.is_file():
            pair = entity.read_text().strip().split('=')
            files[pair[0]] = pair[1]
    return files

# minio_secret = get_secret('minio-secret')
# print(minio_secret)

# client = Minio("minio", minio_secret['accessKey'], minio_secret['secretKey'])

# buckets = client.list_buckets()
# for bucket in buckets:
#     print(bucket.name, bucket.creation_date)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin-user@wavyhealth.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_all_patients(**kwargs):
    sql = "SELECT id, last_checked FROM patient;"
    pg_hook = PostgresHook(postgres_conn_id='patient-database', schema='patient')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    patients = cursor.fetchall()
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='patients', value=list(map(lambda patient: json.dumps(patient, cls=DateTimeEncoder), patients)))

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

def get_all_filtered_patients(**kwargs):
    task_instance = kwargs['task_instance']
    patients = task_instance.xcom_pull(task_ids='get_all_patients', key='patients')
    print(patients)
    filtered_patients = map(lambda patient: days_between(patient[0], datetime.now()) > 7, patients)
    print(filtered_patients)
    task_instance.xcom_push(key='filtered_patients', value=map(lambda filtered_patient: json.dumps(filtered_patient, cls=DateTimeEncoder), filtered_patients))

def mark_patients(**kwargs):
    task_instance = kwargs['task_instance']
    filtered_patients = task_instance.xcom_pull(task_ids='get_all_filtered_patients', key='filtered_patients')
    placeholder = '?'
    placeholders = ', '.join(placeholder * len(filtered_patients))
    sql = "UPDATE patient SET last_checked = CURRENT_TIMESTAMP WHERE id IN (%s);" % placeholders
    pg_hook = PostgresHook(postgres_conn_id='patient-database', schema='patient')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql, map(lambda filtered_patient: filtered_patient[0], filtered_patients))
    patients = cursor.fetchall()

with DAG(
    'train_and_save_personal_ai_models',
    default_args=default_args,
    description='A DAG to train and save personal AI models',
    schedule_interval='@once',
    start_date=days_ago(2),
    tags=['train', 'save', 'ai_models', 'kuberenetes'],
) as dag:
    # 1. [PythonOperator] Get patients 
    get_all_patients = PythonOperator(
        task_id='get_all_patients',
        python_callable=get_all_patients
    )

    # # 2. [PythonOperator] Get filtered patients
    get_all_filtered_patients = PythonOperator(
        task_id='get_all_filtered_patients',
        python_callable=get_all_filtered_patients
    )

    mark_patients = PythonOperator(
        task_id='mark_patients',
        python_callable=mark_patients
    )

    # # 3. [PythonOperator] Filter patients if they have no new files
    # processed_patients = []

    # # 4. [PythonOperator] Preprocess data for each patient
    # processed_logs = []

    # # 5. [KubernetesPodOperator] Train and save workflow
    # processing_tasks = []
    # for patient in processed_patients:
    #     processing_tasks.append(KubernetesPodOperator(
    #         task_id='train_and_save_personal_model',
    #         name='Train and save a personal AI model for a patient',
    #         namespace='default',
    #         image="python",
    #         cmds=["python", "-c"],
    #         arguments=["print('1')"],
    #         labels={"foo": "bar"},
    #         image_pull_policy="Always",
    #         is_delete_operator_pod=True,
    #         get_logs=True,
    #         dag=dag
    #     ))

    # # 6. [PythonOperator] Mark each patient with current date


    # get_patients >> get_logs >> process_patients >> process_logs >> processing_tasks >> mark_patients
    get_all_patients >> get_all_filtered_patients >> mark_patients

