from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain, cross_downstream
default_args = {"retry": 2, "retry_delay": timedelta(minutes=2)}


def _downloading_data(**kwargs):
    print("Downloading the data.....")
    print(kwargs)
    with open("/tmp/my_file.txt", "w") as f:
        f.write("my_data")
    return 42

def _checking_data(ti):
    print("checking the data ..........")

def _failure(context):
    print("On callback failure")


with DAG(
    dag_id="simple_dag",
    start_date=days_ago(3),
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    downloading_data = PythonOperator(
        task_id="downloading_data", python_callable=_downloading_data
    )

    checking_data = PythonOperator(task_id="checking_data", python_callable=_checking_data)

    waiting_for_data = FileSensor(
        task_id="waiting_for_data", fs_conn_id="fs_default", filepath="my_file.txt"
    )

    processing_data = BashOperator(task_id="processing_data", bash_command="exit 1", on_failure_callback=_failure)

    cross_downstream([downloading_data,checking_data],[waiting_for_data,processing_data])
