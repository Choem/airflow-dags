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


class Patient(object):
    __type__ = 'Patient'

    def __init__(self, id, last_checked):
        self.id = id
        self.last_checked = last_checked

    def to_dict(self):
        return { 
            'id': self.id, 
            'last_checked': self.last_checked 
        }

class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                             *args, **kargs)
    
    def dict_to_object(self, d): 
        if '__type__' not in d:
            return d

        type = d.pop('__type__')
        try:
            dateobj = datetime(**d)
            return dateobj
        except:
            d['__type__'] = type
            return d

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__' : 'datetime',
                'year' : obj.year,
                'month' : obj.month,
                'day' : obj.day,
                'hour' : obj.hour,
                'minute' : obj.minute,
                'second' : obj.second,
                'microsecond' : obj.microsecond,
            }   
        else:
            return json.JSONEncoder.default(self, obj)

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
    schedule_interval='@once',
    start_date=days_ago(2),
    tags=['train', 'save', 'ai_models', 'kuberenetes'],
) as dag:
    # 1. [PythonOperator] Get patients
    def get_all_patients(**kwargs):
        sql = "SELECT id, last_checked FROM patient;"
        pg_hook = PostgresHook(postgres_conn_id='patient-database', schema='patient')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        patients = cursor.fetchall()
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(key='patients', value=list(map(lambda patient: json.dumps(patient, cls=DateTimeEncoder), patients)))

    get_all_patients = PythonOperator(
        task_id='get_all_patients',
        python_callable=get_all_patients
    )

    # # 2. [PythonOperator] Get filtered patients
    def get_all_filtered_patients(**kwargs):
        task_instance = kwargs['task_instance']
        patients = list(map(lambda patient: json.loads(patient, cls=DateTimeDecoder), task_instance.xcom_pull(task_ids='get_all_patients', key='patients')))
        # filtered_patients = list(map(lambda patient: (datetime.now() - patient[1]).days >= 7, patients))
        filtered_patients = patients
        task_instance.xcom_push(key='filtered_patients', value=list(map(lambda filtered_patient: json.dumps(filtered_patient, cls=DateTimeEncoder), filtered_patients)))

    get_all_filtered_patients = PythonOperator(
        task_id='get_all_filtered_patients',
        python_callable=get_all_filtered_patients
    )

    def mark_patients(**kwargs):
        task_instance = kwargs['task_instance']
        filtered_patients = list(map(lambda patient: json.loads(patient, cls=DateTimeDecoder), task_instance.xcom_pull(task_ids='get_all_filtered_patients', key='filtered_patients')))
        params = list(map(lambda filtered_patient: filtered_patient[0], filtered_patients))
        sql = "UPDATE patient SET last_checked = CURRENT_TIMESTAMP WHERE id IN (%s);" % ','.join(params)
        pg_hook = PostgresHook(postgres_conn_id='patient-database', schema='patient')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)

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

