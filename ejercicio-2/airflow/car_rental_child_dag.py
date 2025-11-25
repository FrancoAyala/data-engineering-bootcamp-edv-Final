from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

BOOTSTRAP = r"""
set -e
export HADOOP_HOME=/home/hadoop/hadoop
export HIVE_HOME=/home/hadoop/hive
export PATH="$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin"
export HADOOP_USER_NAME=hadoop
which hdfs >/dev/null || { echo 'NO HDFS EN PATH'; exit 127; }
"""

SPARK_ENV = r"""
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH="$PATH:$SPARK_HOME/bin:/home/hadoop/spark/bin"
which spark-submit >/dev/null || { echo 'NO SPARK-SUBMIT EN PATH'; exit 127; }
"""

default_args = {
    "owner": "franco",
    "start_date": days_ago(1),
    "retries": 0,
}

with DAG(
    dag_id="car_rental_child",
    description="Procesa CarRental y deja curated Parquet para Hive",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["car", "child", "spark", "hive"],
) as dag:

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=f"""{BOOTSTRAP}
{SPARK_ENV}
spark-submit /home/hadoop/scripts/transformation_car_rental.py
"""
    )

    spark_transform
