import sys, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

MASTER = sys.argv[1] if len(sys.argv) > 1 else "local[*]"
BASE = "/home/aramati-chiru-thejaswi/data"
TRIPS = f"{BASE}/trips/*.parquet"
LOOKUP = f"{BASE}/lookup/taxi_zone_lookup.csv"
OUT = f"{BASE}/output/spark_out"

spark = (SparkSession.builder.appName("spark_clean").master(MASTER)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.executor.memory", "1g")
         .config("spark.executor.cores", "2")
         .config("spark.cores.max", "4")
         .getOrCreate())

COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
        "fare_amount", "total_amount"]

t0 = time.time()

# 1. Ingest
df = spark.read.parquet(TRIPS).select(*COLS)

# 2. Cleanse
df = df.dropna()
if "--dedup" in sys.argv:
    df = df.dropDuplicates()
dur = (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
       - F.col("tpep_pickup_datetime").cast("timestamp").cast("long"))
df = (df.withColumn("duration_sec", dur)
        .filter((F.col("duration_sec") > 0) & (F.col("duration_sec") <= 21600)
                & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100)
                & (F.col("passenger_count") >= 1))
        .withColumn("pickup_date", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")))

# 3. Heavy join (twice)
zones = spark.read.csv(LOOKUP, header=True, inferSchema=True)
pu = zones.select(F.col("LocationID").alias("PULocationID"),
                  F.col("Borough").alias("pickup_borough"),
                  F.col("Zone").alias("pickup_zone"))
do = zones.select(F.col("LocationID").alias("DOLocationID"),
                  F.col("Borough").alias("dropoff_borough"),
                  F.col("Zone").alias("dropoff_zone"))
df = df.join(F.broadcast(pu), "PULocationID", "inner").join(F.broadcast(do), "DOLocationID", "inner")

# 4. Python UDF
@F.udf(DoubleType())
def avg_speed(dist, secs):
    if dist is None or secs is None or secs <= 0:
        return None
    return round(dist / (secs / 3600.0), 2)

df = df.withColumn("avg_speed_mph", avg_speed("trip_distance", "duration_sec"))

# 5. Export (this triggers the whole pipeline)
df.write.mode("overwrite").parquet(OUT)

print(f"Spark total wall-clock: {time.time() - t0:.2f} s")
print("Rows written:", spark.read.parquet(OUT).count())
input("Done. Take screenshots at http://localhost:4040, then press Enter to exit...")
spark.stop()
