import os
import sys
import time

os.environ["RAY_IGNORE_UNHANDLED_ERRORS"] = "1"  # silence Ray's stats-actor noise

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import ray
import ray.data
from ray.data.context import DataContext

BASE = "/home/aramati-chiru-thejaswi/data"
TRIPS = f"{BASE}/trips"
LOOKUP = f"{BASE}/lookup/taxi_zone_lookup.csv"
OUT = f"{BASE}/output/ray_out"

COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
        "fare_amount", "total_amount"]

ray.init(address="127.0.0.1:6379")
DataContext.get_current().enable_progress_bars = False

# Zone lookup: tiny table, loaded once on the driver.
# keep_default_na=False so "N/A" stays a string, exactly like Spark.
zones = pd.read_csv(LOOKUP, usecols=["LocationID", "Borough", "Zone"],
                    keep_default_na=False)
loc_id = pa.array(zones["LocationID"].astype("int32"), pa.int32())
PU_TBL = pa.table({"PULocationID": loc_id,
                   "pickup_borough": pa.array(zones["Borough"], pa.string()),
                   "pickup_zone": pa.array(zones["Zone"], pa.string())})
DO_TBL = pa.table({"DOLocationID": loc_id,
                   "dropoff_borough": pa.array(zones["Borough"], pa.string()),
                   "dropoff_zone": pa.array(zones["Zone"], pa.string())})

t0 = time.time()

# ---------------------------------------------------------------
# 1. Ingest (explicit columns, so the extra request_source column is dropped)
# ---------------------------------------------------------------
ds = ray.data.read_parquet(TRIPS).select_columns(COLS)


# ---------------------------------------------------------------
# 2. Cleanse: nulls + validity filters
# ---------------------------------------------------------------
def cleanse(t: pa.Table) -> pa.Table:
    t = t.drop_null()
    # same zone-id type in every file (needed for the join)
    t = t.set_column(t.schema.get_field_index("PULocationID"), "PULocationID",
                     pc.cast(t["PULocationID"], pa.int32()))
    t = t.set_column(t.schema.get_field_index("DOLocationID"), "DOLocationID",
                     pc.cast(t["DOLocationID"], pa.int32()))
    pu = pc.cast(t["tpep_pickup_datetime"], pa.timestamp("s"))
    do = pc.cast(t["tpep_dropoff_datetime"], pa.timestamp("s"))
    dur = pc.subtract(pc.cast(do, pa.int64()), pc.cast(pu, pa.int64()))
    mask = pc.and_(
        pc.and_(pc.greater(dur, 0), pc.less_equal(dur, 21600)),
        pc.and_(
            pc.and_(pc.greater(t["trip_distance"], 0),
                    pc.less_equal(t["trip_distance"], 100)),
            pc.greater_equal(t["passenger_count"], 1)))
    return t.filter(mask)


ds = ds.map_batches(cleanse, batch_format="pyarrow")

# Optional dedup on the 9 selected columns (global shuffle: memory-hungry).
# Run with:  python3 ray_clean.py --dedup
if "--dedup" in sys.argv:
    ds = ds.groupby(COLS).count().select_columns(COLS)


# Derived columns
def derive(t: pa.Table) -> pa.Table:
    pu = pc.cast(t["tpep_pickup_datetime"], pa.timestamp("s"))
    do = pc.cast(t["tpep_dropoff_datetime"], pa.timestamp("s"))
    dur = pc.subtract(pc.cast(do, pa.int64()), pc.cast(pu, pa.int64()))
    t = t.append_column("duration_sec", dur)
    t = t.append_column("pickup_date", pc.strftime(pu, format="%Y-%m-%d"))
    t = t.append_column("pickup_hour", pc.cast(pc.hour(pu), pa.int32()))
    return t


ds = ds.map_batches(derive, batch_format="pyarrow")


# ---------------------------------------------------------------
# 3. Join (twice, inner): map-side join against the small zone table
# ---------------------------------------------------------------
def join_pickup(t: pa.Table) -> pa.Table:
    return t.join(PU_TBL, keys="PULocationID", join_type="inner")


def join_dropoff(t: pa.Table) -> pa.Table:
    return t.join(DO_TBL, keys="DOLocationID", join_type="inner")


ds = ds.map_batches(join_pickup, batch_format="pyarrow")
ds = ds.map_batches(join_dropoff, batch_format="pyarrow")


# ---------------------------------------------------------------
# 4. Python UDF (plain Python function, same formula as Spark)
# ---------------------------------------------------------------
def avg_speed(dist, secs):
    if dist is None or secs is None or secs <= 0:
        return None
    return round(dist / (secs / 3600.0), 2)


def add_speed(batch: pd.DataFrame) -> pd.DataFrame:
    batch["avg_speed_mph"] = [avg_speed(d, s) for d, s in
                              zip(batch["trip_distance"], batch["duration_sec"])]
    return batch


ds = ds.map_batches(add_speed, batch_format="pandas")

# ---------------------------------------------------------------
# 5. Export (this triggers the whole pipeline)
# ---------------------------------------------------------------
ds.write_parquet(OUT)

print(f"Ray total wall-clock: {time.time() - t0:.2f} s")
print("Rows written:", ray.data.read_parquet(OUT).count())

input("Done. Take screenshots at http://localhost:8265, then press Enter to exit...")
ray.shutdown()
