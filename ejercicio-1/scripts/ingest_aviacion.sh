#!/bin/bash
set -e

echo "=========================================="
echo "✈️  AVIACIÓN CIVIL - INGESTA DE ARCHIVOS"
echo "=========================================="

LANDING=/home/hadoop/landing

echo "📁 Creando carpeta landing..."
mkdir -p $LANDING || true
echo "✔️ Carpeta creada: $LANDING"

echo "⬇️ Descargando archivos..."
wget -c -P $LANDING https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv
wget -c -P $LANDING https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv
wget -c -P $LANDING https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv
echo "✔️ Descargas completas"

echo "📤 Subiendo archivos a HDFS..."
hdfs dfs -mkdir -p /ingest || true
hdfs dfs -put -f $LANDING/2021-informe-ministerio.csv /ingest/
hdfs dfs -put -f $LANDING/202206-informe-ministerio.csv /ingest/
hdfs dfs -put -f $LANDING/aeropuertos_detalle.csv /ingest/

echo "=========================================="
echo "✔️ INGESTA COMPLETADA EXITOSAMENTE"
echo "=========================================="
