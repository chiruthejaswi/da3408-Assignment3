# Spark vs. Ray: (DA3408 Assignment3)

The same NYC Yellow Taxi cleaning pipeline (20 Parquet files, 2025-01 to 2026-08, 57,068,386 clean rows) in PySpark and Ray Data, each on a 2-worker cluster.

## Files
- `spark_clean.py`: PySpark pipeline
- `ray_clean.py`: Ray Data pipeline
- `check_data.py`: schema and data-quality check on the raw files
- `compare_outputs.py`: value-by-value parity check of the two outputs
- `evidence/parity_report.txt`: parity check result (outputs match exactly)
- `screenshots/`: Spark Master UI (2 workers), Ray Dashboard (cluster view), Spark UI pages
- `Spark_vs_Ray_Report.pdf`: benchmark report

## Pipeline (identical in both)
1. Read 20 Parquet files, selecting 9 columns.
2. Drop nulls; keep rows with dropoff after pickup, duration at most 6 h, 0 < distance at most 100 miles, passengers at least 1.
3. Add duration_sec, pickup_date, pickup_hour (UTC).
4. Inner join with the zone lookup (pickup and dropoff).
5. Python UDF: avg_speed_mph = round(trip_distance / (duration_sec / 3600), 2).
6. Export to Parquet.

## Clusters (2 workers each, one 4-core VM)
- Spark 4.2.0 standalone: master + 2 workers (2 cores, 1 GB each).
- Ray 2.58.0: head (0 CPU) + 2 workers (2 CPU each).

## Run
    python3 spark_clean.py spark://127.0.0.1:7077
    python3 ray_clean.py
    python3 compare_outputs.py

Raw data is not included (source: NYC TLC Trip Record Data).
