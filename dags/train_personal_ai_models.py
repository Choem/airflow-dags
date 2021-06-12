from pathlib import Path
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

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

with DAG(
    'train_and_save_personal_ai_models',
    default_args=default_args,
    description='A DAG to train and save personal AI models',
    schedule_interval='* 1 * * *',
    start_date=days_ago(2),
    tags=['train', 'save', 'ai_models', 'kuberenetes'],
) as dag:
    # 1. [PythonOperator] Get patients 
    patients = []

    def t1_callable(**kwargs):
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(key='test', value=[1, "tweee", { "kek": "kek" }])

    t1 = PythonOperator(
        task_id='t1',
        provide_context=True,
        python_callable=t1_callable,
        dag=dag
    )

    def t2_callable(**kwargs):
        task_instance = kwargs['task_instance']
        value = task_instance.xcom_pull(task_ids='t1', key='test')
        print(value)
        print(t1[0] * t[1])

    t2 = PythonOperator(
        task_id='t2',
        provide_context=True,
        python_callable=t2_callable,
        dag=dag
    )

    # # 2. [PythonOperator] Get logs from the patients in user buckets
    # logs = []

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
    t1 >> t2

