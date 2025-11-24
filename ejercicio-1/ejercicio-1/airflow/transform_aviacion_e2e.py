from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

# === Paths/vars ===
landing_dir = Variable.get("landing_dir", default_var="/home/hadoop/landing")
hdfs_ingest = Variable.get("hdfs_ingest", default_var="/ingest")

url_2021   = "https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv"
url_202206 = "https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv"
url_det    = "https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv"

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
    "depends_on_past": False,
}

with DAG(
    dag_id="transform_aviacion_e2e",
    description="HDFS -> Spark (limpieza) -> Hive (stage CSV -> finales tipadas)",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["edv", "hdfs", "spark", "hive", "ingest"],
) as dag:

    start_process = DummyOperator(task_id="start_process")

    prep_folders = BashOperator(
        task_id="prep_folders",
        bash_command=f"""{BOOTSTRAP}
mkdir -p {landing_dir} || true
hdfs dfs -mkdir -p {hdfs_ingest} || true
hdfs dfs -mkdir -p {hdfs_ingest}/clean || true
""",
    )

    download_files = BashOperator(
        task_id="download_files",
        bash_command=f"""{BOOTSTRAP}
wget -c -P {landing_dir} {url_2021}
wget -c -P {landing_dir} {url_202206}
wget -c -P {landing_dir} {url_det}
""",
    )

    push_to_hdfs = BashOperator(
        task_id="push_to_hdfs",
        bash_command=f"""{BOOTSTRAP}
hdfs dfs -put -f {landing_dir}/2021-informe-ministerio.csv   {hdfs_ingest}/
hdfs dfs -put -f {landing_dir}/202206-informe-ministerio.csv {hdfs_ingest}/
hdfs dfs -put -f {landing_dir}/aeropuertos_detalle.csv       {hdfs_ingest}/
hdfs dfs -ls -h {hdfs_ingest}
""",
    )

    create_db = BashOperator(
        task_id="create_db",
        bash_command=f"""{BOOTSTRAP}
hive -e "CREATE DATABASE IF NOT EXISTS dw_aviacion;"
""",
    )

    spark_transform_limpieza = BashOperator(
        task_id="spark_transform_limpieza",
        bash_command=f"""{BOOTSTRAP}
{SPARK_ENV}
spark-submit /home/hadoop/scripts/transformation_aviacion.py
""",
    )

    ls_clean = BashOperator(
        task_id="ls_clean",
        bash_command=f"""{BOOTSTRAP}
hdfs dfs -ls -h {hdfs_ingest}/clean || true
hdfs dfs -ls -h {hdfs_ingest}/clean/vuelos || true
hdfs dfs -ls -h {hdfs_ingest}/clean/aeropuertos || true
""",
    )

    create_stage_tables = BashOperator(
        task_id="create_stage_tables",
        bash_command=f"""{BOOTSTRAP}
hive -e "
USE dw_aviacion;

DROP TABLE IF EXISTS vuelos_ar_stage_csv;
CREATE TABLE vuelos_ar_stage_csv (
  fecha STRING,
  horautc STRING,
  clase_de_vuelo STRING,
  clasificacion_de_vuelo STRING,
  tipo_de_movimiento STRING,
  aeropuerto STRING,
  origen_destino STRING,
  aerolinea_nombre STRING,
  aeronave STRING,
  pasajeros STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS aeropuertos_ar_stage_csv;
CREATE TABLE aeropuertos_ar_stage_csv (
  aeropuerto STRING,
  oac STRING,
  iata STRING,
  tipo STRING,
  denominacion STRING,
  coordenadas STRING,
  latitud STRING,
  longitud STRING,
  elev STRING,
  uom_elev STRING,
  ref STRING,
  distancia_ref STRING,
  direccion_ref STRING,
  condicion STRING,
  control STRING,
  region STRING,
  uso STRING,
  trafico STRING,
  sna STRING,
  concesionado STRING,
  provincia STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;
"
""",
    )

    load_stage = BashOperator(
        task_id="load_stage",
        bash_command=f"""{BOOTSTRAP}
FSURI=$(hdfs getconf -confKey fs.defaultFS)

hdfs dfs -test -e {hdfs_ingest}/clean/vuelos       || {{ echo 'NO existe {hdfs_ingest}/clean/vuelos';       hdfs dfs -ls -R {hdfs_ingest}; exit 64; }}
hdfs dfs -test -e {hdfs_ingest}/clean/aeropuertos  || {{ echo 'NO existe {hdfs_ingest}/clean/aeropuertos';  hdfs dfs -ls -R {hdfs_ingest}; exit 64; }}

hive -e "
USE dw_aviacion;

TRUNCATE TABLE vuelos_ar_stage_csv;
TRUNCATE TABLE aeropuertos_ar_stage_csv;

LOAD DATA INPATH '${{FSURI}}{hdfs_ingest}/clean/vuelos'       INTO TABLE vuelos_ar_stage_csv;
LOAD DATA INPATH '${{FSURI}}{hdfs_ingest}/clean/aeropuertos'  INTO TABLE aeropuertos_ar_stage_csv;
"
""",
    )

    build_final_tables = BashOperator(
        task_id="build_final_tables",
        bash_command=f"""{BOOTSTRAP}
hive -e "
USE dw_aviacion;

DROP TABLE IF EXISTS vuelos_ar_clean;
CREATE TABLE vuelos_ar_clean
STORED AS PARQUET
AS
SELECT
  COALESCE(
    to_date(from_unixtime(unix_timestamp(fecha,'dd/MM/yyyy'))),
    to_date(fecha)
  )                                    AS fecha,
  horautc                               AS horautc,
  clase_de_vuelo                        AS clase_de_vuelo,
  clasificacion_de_vuelo                AS clasificacion_de_vuelo,
  tipo_de_movimiento                    AS tipo_de_movimiento,
  aeropuerto                            AS aeropuerto,
  origen_destino                        AS origen_destino,
  aerolinea_nombre                      AS aerolinea_nombre,
  aeronave                              AS aeronave,
  CAST(regexp_replace(pasajeros, '[^0-9]', '') AS INT) AS pasajeros
FROM vuelos_ar_stage_csv;

DROP TABLE IF EXISTS aeropuertos_ar_clean;
CREATE TABLE aeropuertos_ar_clean
STORED AS PARQUET
AS
SELECT
  aeropuerto,
  oac,
  iata,
  tipo,
  denominacion,
  coordenadas,
  latitud,
  longitud,
  CAST(regexp_replace(elev, ',', '.') AS FLOAT)          AS elev,
  uom_elev,
  ref,
  CAST(regexp_replace(distancia_ref, ',', '.') AS FLOAT) AS distancia_ref,
  direccion_ref,
  condicion,
  control,
  region,
  uso,
  trafico,
  sna,
  concesionado,
  provincia
FROM aeropuertos_ar_stage_csv;
"
""",
    )

    finish_process = DummyOperator(task_id="finish_process")

    # Orquestación
    start_process >> prep_folders >> download_files >> push_to_hdfs >> create_db
    create_db >> spark_transform_limpieza >> ls_clean
    ls_clean >> create_stage_tables >> load_stage >> build_final_tables >> finish_process
