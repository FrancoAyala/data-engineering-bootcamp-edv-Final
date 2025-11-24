-- =========================================================
--  CREACIÓN DE BASE Y TABLAS PARA EJERCICIO 1 - AVIACIÓN
--  Autor: Franco Ayala
--  Descripción: Esquema usado en el pipeline Airflow + Spark + Hive
-- =========================================================

-- 1) Base de datos
CREATE DATABASE IF NOT EXISTS dw_aviacion;
USE dw_aviacion;

-- Limpieza previa (por si se recrea el entorno)
DROP TABLE IF EXISTS vuelos_ar_stage_csv;
DROP TABLE IF EXISTS aeropuertos_ar_stage_csv;
DROP TABLE IF EXISTS vuelos_ar_clean;
DROP TABLE IF EXISTS aeropuertos_ar_clean;

-- =========================================================
-- 2) Tablas STAGE en CSV (todas las columnas STRING)
--    Estas son las tablas donde aterrizan los datos limpios
--    generados por PySpark desde HDFS (/ingest/clean).
-- =========================================================

-- Stage de vuelos (unión 2021 + 2022)
CREATE TABLE vuelos_ar_stage_csv (
  fecha              STRING,
  horautc            STRING,
  clase_de_vuelo     STRING,
  clasificacion_de_vuelo STRING,
  tipo_de_movimiento STRING,
  aeropuerto         STRING,
  origen_destino     STRING,
  aerolinea_nombre   STRING,
  aeronave           STRING,
  pasajeros          STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;

-- Stage de aeropuertos
CREATE TABLE aeropuertos_ar_stage_csv (
  aeropuerto     STRING,
  oac            STRING,
  iata           STRING,
  tipo           STRING,
  denominacion   STRING,
  coordenadas    STRING,
  latitud        STRING,
  longitud       STRING,
  elev           STRING,
  uom_elev       STRING,
  ref            STRING,
  distancia_ref  STRING,
  direccion_ref  STRING,
  condicion      STRING,
  control        STRING,
  region         STRING,
  uso            STRING,
  trafico        STRING,
  sna            STRING,
  concesionado   STRING,
  provincia      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;

-- =========================================================
-- 3) Tablas FINALES tipadas en formato PARQUET
--    Estas son las tablas que se usan para el análisis.
-- =========================================================

-- Vuelos limpios
CREATE TABLE vuelos_ar_clean
STORED AS PARQUET
AS
SELECT
  COALESCE(
    to_date(from_unixtime(unix_timestamp(fecha,'dd/MM/yyyy'))),
    to_date(fecha)
  )                                         AS fecha,
  horautc                                   AS horautc,
  clase_de_vuelo                            AS clase_de_vuelo,
  clasificacion_de_vuelo                    AS clasificacion_de_vuelo,
  tipo_de_movimiento                        AS tipo_de_movimiento,
  aeropuerto                                AS aeropuerto,
  origen_destino                            AS origen_destino,
  aerolinea_nombre                          AS aerolinea_nombre,
  aeronave                                  AS aeronave,
  CAST(regexp_replace(pasajeros,'[^0-9]','') AS INT) AS pasajeros
FROM vuelos_ar_stage_csv;

-- Aeropuertos limpios
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
