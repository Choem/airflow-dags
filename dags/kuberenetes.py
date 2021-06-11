from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

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
    'kubernetes',
    default_args=default_args,
    description='A Kubernetes DAG',
    schedule_interval='1/5 * * * *',
    start_date=days_ago(2),
    tags=['kubernetes'],
) as dag:
    t1 = KubernetesPodOperator(
        namespace='default',
        image="python",
        cmds=["python", "-c"],
        arguments=["print('1')"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        name='test_1',
        task_id='test_1',
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )

    t2 = KubernetesPodOperator(
        namespace='default',
        image="python",
        cmds=["python", "-c"],
        arguments=["print('2')"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        name='test_2',
        task_id='test_2',
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )

    t1 >> t2

