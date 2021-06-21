# Standard imports
import json
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from pathlib import Path

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator 
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

# Gql; For calling our services
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['email@test.com'],
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
    tags=['train', 'save', 'ai_models', 'kuberenetes', 'v37'],
) as dag:
    # Bug with Airflow, secret gets mounted but not populated
    # minio_accesskey = Path("/opt/airflow/secrets/minio-secret/acceskey").read_text().strip()
    # minio_secretkey = Path("/opt/airflow/secrets/minio-secret/secretkey").read_text().strip()

    # Gets the patient ids from the patient service
    def get_patient_ids():
        # Define transport protocol
        transport = RequestsHTTPTransport(
            url='http://file-service:4000/file/graphql/query', 
            use_json=True,
        )

        # Create GraphQL client
        client = Client(transport=transport, fetch_schema_from_transport=True)

        # Define query
        query = gql("""
            query GetActivePatients($startDate: String!, $endDate: String!) {
                getActivePatients(startDate: $startDate, endDate: $endDate)
            }
        """)

        # Define parameters
        now = datetime.now()
        one_week_ago = now - timedelta(days=7)

        params = { "startDate": one_week_ago.isoformat(), "endDate": now.isoformat() }
        
        # Execute query
        result = client.execute(query, variable_values=params)
        
        # Get patient ids
        return result['getActivePatients']

    # Enumerate over patient ids returned from the file service query
    for index, patient_id in enumerate(get_patient_ids()):
        with TaskGroup(group_id='train_and_save_model_group_%s' % index) as task_group:
            start_task_group = DummyOperator(
                task_id='start_task_group_%s' % index,
                dag=dag
            )

            # Start up a k8s pod to train and save personal AI model
            train_and_save_model_task_group = KubernetesPodOperator(
                task_id='train_and_save_model_task_group_%s' % index,
                name='train_and_save_model_task_group_%s' % index,
                namespace='default',
                env_vars={ 
                    'PATIENT_ID': str(patient_id),
                    'MINIO_ACCESS_KEY': 'admin-user',
                    'MINIO_SECRET_KEY': 'admin-user'
                },
                image="k3d-airflow-backend-registry:5000/train_personal_ai_model:v23",
                image_pull_policy="IfNotPresent",
                is_delete_operator_pod=True,
                get_logs=True,
                dag=dag
            )

            end_task_group = DummyOperator(
                task_id='end_task_group_%s' % index,
                dag=dag
            )

            start_task_group >> train_and_save_model_task_group >> end_task_group