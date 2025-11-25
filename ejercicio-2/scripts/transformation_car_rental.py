from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, coalesce
from pyspark.sql.functions import round as sround
from pyspark.sql.types import IntegerType, StringType

RAW_DATA = "hdfs:///car_rental/raw/CarRentalData.csv"
RAW_STATES = "hdfs:///car_rental/raw/us_states.csv"
OUT_PATH = "hdfs:///car_rental/curated/analytics/"

spark = (
    SparkSession.builder
    .appName("car_rental_transform")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("SET mapreduce.fileoutputcommitter.marksuccessfuljobs=false")

cars = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(RAW_DATA)
)

states_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(RAW_STATES)
)

def safe(n):
    return n.strip().replace(" ", "_").replace(".", "_")

cars = cars.toDF(*[safe(c) for c in cars.columns])
states_raw = states_raw.toDF(*[safe(c) for c in states_raw.columns])

cars = cars.select(
    trim(col("fuelType")).alias("fuelType"),
    trim(col("rating")).alias("rating"),
    trim(col("renterTripsTaken")).alias("renterTripsTaken"),
    trim(col("reviewCount")).alias("reviewCount"),
    trim(col("city")).alias("city"),
    (trim(col("state_name")) if "state_name" in cars.columns else trim(col("state"))).alias("state_name_tmp"),
    trim(col("owner_id")).alias("owner_id"),
    trim(col("rate_daily")).alias("rate_daily"),
    trim(col("make")).alias("make"),
    trim(col("model")).alias("model"),
    trim(col("year")).alias("year"),
)

state_name_col = (
    "name" if "name" in states_raw.columns
    else ("label_en" if "label_en" in states_raw.columns else None)
)

if state_name_col:
    states = states_raw.select(trim(col(state_name_col)).alias("state_name_ref"))
else:
    from pyspark.sql.functions import lit
    states = states_raw.select(lit(None).cast(StringType()).alias("state_name_ref"))

cars = cars.withColumn("rating", sround(col("rating")).cast(IntegerType()))
cars = cars.where(col("rating").isNotNull())
cars = cars.withColumn("fuelType", lower(col("fuelType")).cast(StringType()))

cars = (
    cars.alias("c")
    .join(
        states.alias("s"),
        lower(col("c.state_name_tmp")) == lower(col("s.state_name_ref")),
        "left",
    )
)

cars = cars.withColumn(
    "state_name",
    coalesce(col("s.state_name_ref"), col("c.state_name_tmp"))
).drop("state_name_tmp")

cars = cars.where(lower(col("state_name")) != "texas")

cars = (
    cars
    .withColumn("renterTripsTaken", col("renterTripsTaken").cast(IntegerType()))
    .withColumn("reviewCount",      col("reviewCount").cast(IntegerType()))
    .withColumn("owner_id",         col("owner_id").cast(IntegerType()))
    .withColumn("rate_daily",       col("rate_daily").cast(IntegerType()))
    .withColumn("year",             col("year").cast(IntegerType()))
    .select(
        "fuelType",
        "rating",
        "renterTripsTaken",
        "reviewCount",
        "city",
        "state_name",
        "owner_id",
        "rate_daily",
        "make",
        "model",
        "year",
    )
)

cars.repartition(1).write.mode("overwrite").parquet(OUT_PATH)

spark.stop()
