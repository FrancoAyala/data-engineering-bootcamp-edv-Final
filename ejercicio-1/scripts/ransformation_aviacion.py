from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace

# Rutas RAW en HDFS
RAW_PATH_2021   = "hdfs:///ingest/2021-informe-ministerio.csv"
RAW_PATH_202206 = "hdfs:///ingest/202206-informe-ministerio.csv"
RAW_PATH_AER    = "hdfs:///ingest/aeropuertos_detalle.csv"

# Rutas CLEAN en HDFS
OUT_VUELOS = "hdfs:///ingest/clean/vuelos"
OUT_AER    = "hdfs:///ingest/clean/aeropuertos"

spark = (
    SparkSession.builder
    .appName("aviacion_limpieza_hdfs")
    .enableHiveSupport()
    .getOrCreate()
)

# ==============================
# LECTURA DE ARCHIVOS
# ==============================
schema_opts = {
    "header": True,
    "sep": ";",
    "inferSchema": False,
    "quote": '"',
    "escape": '"',
    "multiLine": False
}

v2021   = spark.read.options(**schema_opts).csv(RAW_PATH_2021)
v202206 = spark.read.options(**schema_opts).csv(RAW_PATH_202206)
vuelos_raw = v2021.unionByName(v202206)

# ==============================
# TRANSFORMACIONES VUELOS
# ==============================
vuelos = vuelos_raw.selectExpr(
    "`Fecha`                                 as fecha",
    "`Hora UTC`                              as horaUTC",
    "`Clase de Vuelo (todos los vuelos)`     as clase_de_vuelo",
    "`Clasificación Vuelo`                   as clasificacion_de_vuelo",
    "`Tipo de Movimiento`                    as tipo_de_movimiento",
    "`Aeropuerto`                            as aeropuerto",
    "`Origen / Destino`                      as origen_destino",
    "`Aerolinea Nombre`                      as aerolinea_nombre",
    "`Aeronave`                              as aeronave",
    "`Pasajeros`                             as pasajeros",
    "`Calidad dato`                          as calidad_dato"
)

# Filtro: solo vuelos domésticos
vuelos = vuelos.where(~lower(col("clasificacion_de_vuelo")).contains("intern"))

# Pasajeros: normalizar NULL/vacíos → 0
vuelos = vuelos.withColumn("pasajeros", trim(col("pasajeros")))
vuelos = vuelos.withColumn("pasajeros", regexp_replace(col("pasajeros"), r"[^0-9]", ""))
vuelos = vuelos.withColumn("pasajeros", col("pasajeros").cast("int")).na.fill({"pasajeros": 0})

# Remover columna innecesaria
vuelos = vuelos.drop("calidad_dato")

# ==============================
# AEROPUERTOS
# ==============================
aer = spark.read.options(**schema_opts).csv(RAW_PATH_AER)

aer = aer.selectExpr(
    "`denominacion`  as aeropuerto",
    "`oaci`          as oac",
    "`iata`          as iata",
    "`tipo`          as tipo",
    "`denominacion`  as denominacion",
    "`coordenadas`   as coordenadas",
    "`latitud`       as latitud",
    "`longitud`      as longitud",
    "`elev`          as elev",
    "`uom_elev`      as uom_elev",
    "`ref`           as ref",
    "`distancia_ref` as distancia_ref",
    "`direccion_ref` as direccion_ref",
    "`condicion`     as condicion",
    "`control`       as control",
    "`region`        as region",
    "`uso`           as uso",
    "`trafico`       as trafico",
    "`sna`           as sna",
    "`concesionado`  as concesionado",
    "`provincia`     as provincia"
)

# ==============================
# ESCRITURA A HDFS CLEAN
# ==============================
spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

vuelos.coalesce(1).write.mode("overwrite").option("sep", ";").option("header", "false").csv(OUT_VUELOS)
aer.coalesce(1).write.mode("overwrite").option("sep", ";").option("header", "false").csv(OUT_AER)

spark.stop()
