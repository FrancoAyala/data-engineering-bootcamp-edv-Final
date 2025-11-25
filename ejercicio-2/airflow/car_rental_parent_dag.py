from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BOOTSTRAP = r"""
set -e
export HADOOP_HOME=/home/hadoop/hadoop
export HIVE_HOME=/home/hadoop/hive
export PATH="$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin"
export HADOOP_USER_NAME=hadoop
which hdfs >/dev/null || { echo 'NO HDFS EN PATH'; exit 127; }
"""

default_args = {
    "owner": "franco",
    "start_date": days_ago(1),
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="car_rental_parent",
    description="Ingesta RAW y trigger al DAG hijo",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["car", "parent", "ingest"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=f"""{BOOTSTRAP}
bash /home/hadoop/scripts/ingest_car_rental.sh
"""
    )

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="car_rental_child",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    ingest >> trigger_child
