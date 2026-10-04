import glob
import pandas as pd
import pyarrow.parquet as pq

files = sorted(glob.glob("data/trips/*.parquet"))

schemas = {}
for f in files:
    schemas[f] = tuple((fld.name, str(fld.type)) for fld in pq.read_schema(f))
unique = set(schemas.values())
print(f"{len(files)} files, {len(unique)} distinct schema(s)")
if len(unique) > 1:
    base = set(next(iter(unique)))
    for f, s in schemas.items():
        diff = set(s) ^ base
        if diff:
            print(f, "differs:", diff)

#Inspecting one file in detail
df = pd.read_parquet(files[0])
print("\nRows:", len(df))
print(df.dtypes)
print("\nNulls:\n", df.isna().sum())
print("\nExact duplicate rows:", df.duplicated().sum())
print(df[["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]].describe())