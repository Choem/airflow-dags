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
from airflow.operators.dummy import DummyOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

from minio import Minio

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
    tags=['train', 'save', 'ai_models', 'kuberenetes', 'v15'],
) as dag:
    # Gets the patient ids from the patient service
    def get_patient_ids():
        # GraphQL call
        return [1, 2]

    for index, patient_id in enumerate(get_patient_ids()):
        with TaskGroup(group_id='train_and_save_model_group_%s' % index) as task_group:
            start_task_group = DummyOperator(
                task_id='start_task_group_%s' % index,
                dag=dag
            )

            train_and_save_model_task_group = KubernetesPodOperator(
                task_id='train_and_save_model_task_group_%s' % index,
                name='train_and_save_model_task_group_%s' % index,
                namespace='default',
                # image_pull_secrets=[k8s.V1LocalObjectReference('docker-secret')],
                env_vars={ 
                    'USER_ID': str(patient_id),
                    'MINIO_ACCESS_KEY': 'admin-user',
                    'MINIO_SECRET_KEY': 'admin-user' 
                },
                image="k3d-airflow-backend-registry:5000/train_and_save_personal_model:v1",
                image_pull_policy="Never",
                is_delete_operator_pod=False,
                get_logs=True,
                dag=dag
            )

            end_task_group = DummyOperator(
                task_id='end_task_group_%s' % index,
                dag=dag
            )

            start_task_group >> train_and_save_model_task_group >> end_task_group