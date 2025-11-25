# /home/hadoop/scripts/ingest_car_rental.sh
# Script de ingesta RAW para el ejercicio 2 (Car Rental)

#!/usr/bin/env bash
set -e

# Directorios (parametrizables)
LANDING_DIR=${LANDING_DIR:-/home/hadoop/landing}
HDFS_RAW=${HDFS_RAW:-/car_rental/raw}

# URLs de origen (S3 público)
URL_DATA="https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv"
URL_STATES="https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv"

# Bootstrap Hadoop / Hive
export HADOOP_HOME=/home/hadoop/hadoop
export HIVE_HOME=/home/hadoop/hive
export PATH="$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin"
export HADOOP_USER_NAME=hadoop

echo "[INFO] Creando carpeta de landing: $LANDING_DIR"
mkdir -p "$LANDING_DIR"

echo "[INFO] Descargando CarRentalData.csv..."
wget -c -P "$LANDING_DIR" -O "$LANDING_DIR/CarRentalData.csv" "$URL_DATA"

echo "[INFO] Descargando us_states.csv..."
wget -c -P "$LANDING_DIR" -O "$LANDING_DIR/us_states.csv" "$URL_STATES"

echo "[INFO] Creando carpeta RAW en HDFS: $HDFS_RAW"
hdfs dfs -mkdir -p "$HDFS_RAW"

echo "[INFO] Subiendo archivos a HDFS..."
hdfs dfs -put -f "$LANDING_DIR/CarRentalData.csv" "$HDFS_RAW/"
hdfs dfs -put -f "$LANDING_DIR/us_states.csv"       "$HDFS_RAW/"

echo "[INFO] Contenido final en $HDFS_RAW:"
hdfs dfs -ls -h "$HDFS_RAW"

echo "[OK] Ingesta de datos de Car Rental completada."
