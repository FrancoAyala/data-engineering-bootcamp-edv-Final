# 🚗 Ejercicio 2 – Pipeline de Car Rental (Airflow + Spark + Hive)

Este ejercicio implementa un pipeline ETL completo para procesar datos de alquileres de autos usando:

- Apache Airflow para orquestación  
- PySpark para transformaciones distribuidas  
- Apache Hive como data warehouse  
- HDFS como sistema de archivos distribuido  

---

## 📂 Estructura del ejercicio

El directorio `ejercicio-2/` contiene:

- `airflow/` → DAG Padre y DAG Hijo  
- `hive/` → DDL y tabla externa  
- `scripts/` → Scripts de ingesta y transformación  
- `images/` → Imágenes del DAG e informes de consultas  
- `README.md` → Este archivo  

---

## 🚀 1. DAGs de Airflow

### 🔷 DAG Padre – `car_rental_parent.py`

Ubicación: `airflow/car_rental_parent.py`

Este DAG realiza:

1. Descarga de datasets desde S3  
2. Copia a la carpeta *landing*  
3. Ingesta a HDFS (`/car_rental/raw`)  
4. Trigger del DAG hijo (`car_rental_child`)  

#### 🖼️ Imagen del DAG Padre

![DAG Padre](images/ejercicio2_dagF.png)

---

### 🔶 DAG Hijo – `car_rental_child.py`

Ubicación: `airflow/car_rental_child.py`

Este DAG se encarga de:

- Ejecutar el script de PySpark  
- Limpiar y normalizar columnas  
- Enriquecer con dataset de estados  
- Excluir registros de Texas (requisito del ejercicio)  
- Generar la capa *curated* en formato Parquet en HDFS  

#### 🖼️ Imagen del DAG Hijo

![DAG Hijo](images/ejercicio2_dagC.png)

---

## 🛠️ 2. Scripts utilizados

### 📌 Ingesta (bash)

Ubicación: `scripts/ingest_car_rental.sh`

Responsabilidades principales:

- Descargar `CarRentalData.csv` y `us_states.csv` desde S3  
- Crear carpetas de *landing* y RAW  
- Subir los archivos a `hdfs:///car_rental/raw`  
- Listar el contenido final en HDFS para validación  

---

### 📌 Transformación con PySpark

Ubicación: `scripts/transformation_car_rental.py`

Transformaciones aplicadas:

- Normalización de nombres de columnas  
- `trim` y `lower` sobre cadenas de texto  
- Conversión de tipos a `INT` donde corresponde  
- Join con el dataset de estados de EE.UU.  
- Exclusión de registros de Texas  
- Escritura del dataset final en Parquet en:  
  `hdfs:///car_rental/curated/analytics/`

---

## 🗄️ 3. Tabla Hive

Ubicación del DDL: `hive/create_table_car_rental.sql`

Se crea la base y tabla externa:

- Base: `car_rental_db`  
- Tabla: `car_rental_analytics` (EXTERNAL, PARQUET)  
- Ubicación: `hdfs:///car_rental/curated/analytics/`  

---

## 📊 4. Consultas de análisis (KPIs)

Las consultas de negocio se encuentran en:  
`hive/queries_car_rental.sql`

A continuación se muestran los resultados de cada punto del ejercicio:

### a) Cantidad de alquileres de autos ecológicos (fuelType híbrido o eléctrico, rating ≥ 4)

![Consulta A](images/ejercicio2_a.png)

---

### b) Los 5 estados con menor cantidad de alquileres

![Consulta B](images/ejercicio2_b.png)

---

### c) Los 10 modelos (y marcas) de autos más rentados

![Consulta C](images/ejercicio2_c.png)

---

### d) Cantidad de alquileres por año, para autos fabricados entre 2010 y 2015

![Consulta D](images/ejercicio2_d.png)

---

### e) Las 5 ciudades con más alquileres de vehículos ecológicos

![Consulta E](images/ejercicio2_e.png)

---

### f) Promedio de reviews segmentado por tipo de combustible

![Consulta F](images/ejercicio2_f.png)

---

## 📝 Notas

- La ingesta usa URLs públicas de S3.  
- Los datos RAW se almacenan en `/car_rental/raw`.  
- La capa *curated* se genera en Parquet, optimizada para consulta.  
- Hive lee directamente desde la capa curated sin necesidad de mover datos.  

