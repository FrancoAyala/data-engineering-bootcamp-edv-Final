# Ejercicio 1 – Pipeline de Aviación (Airflow + Spark + Hive)

Este ejercicio implementa un pipeline ETL para procesar datos de aviación argentina usando:

- Apache Airflow para orquestación
- PySpark para transformaciones distribuidas
- Apache Hive como data warehouse
- HDFS como sistema de archivos distribuido

En esta carpeta vas a encontrar:

- `airflow/` → DAG de Airflow (`transform_aviacion_e2e.py`)
- `scripts/` → scripts de ingest y transformación (`ingest_aviacion.sh`, `transformation_aviacion.py`)
- `hive/` → DDL y queries de Hive (`create_tables.sql`)

Más detalles en el README principal del repositorio.
