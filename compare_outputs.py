"""Compare Spark and Ray Parquet outputs value by value (streaming, low memory)."""
import sys
import time
import duckdb

BASE = "/home/aramati-chiru-thejaswi/data/output"
SPARK = sys.argv[1] if len(sys.argv) > 1 else f"{BASE}/spark_out/*.parquet"
RAY = sys.argv[2] if len(sys.argv) > 2 else f"{BASE}/ray_out/*.parquet"

con = duckdb.connect()
con.execute("PRAGMA memory_limit='2GB'")
con.execute("PRAGMA threads=2")
con.execute("PRAGMA temp_directory='/tmp/duckdb_tmp'")
con.execute(f"CREATE VIEW spark AS SELECT * FROM read_parquet('{SPARK}', union_by_name=true)")
con.execute(f"CREATE VIEW ray   AS SELECT * FROM read_parquet('{RAY}',   union_by_name=true)")

ok = True
t0 = time.time()

# 1. Row counts
n_s = con.execute("SELECT count(*) FROM spark").fetchone()[0]
n_r = con.execute("SELECT count(*) FROM ray").fetchone()[0]
print(f"[1] Row count   spark={n_s:,}  ray={n_r:,}  ->", "PASS" if n_s == n_r else "FAIL")
ok &= n_s == n_r

# 2. Columns
cs = [r[0] for r in con.execute("DESCRIBE spark").fetchall()]
cr = [r[0] for r in con.execute("DESCRIBE ray").fetchall()]
only_s, only_r = sorted(set(cs) - set(cr)), sorted(set(cr) - set(cs))
common = sorted(set(cs) & set(cr))
print(f"[2] Columns     spark={len(cs)} ray={len(cr)} common={len(common)}  ->",
      "PASS" if not only_s and not only_r else "FAIL")
if only_s or only_r:
    print("    only in spark:", only_s, " only in ray:", only_r)
    ok = False

# 3. Per-column nulls / min / max
print("[3] Per-column nulls/min/max")
agg = ", ".join(
    f'count(*) FILTER (WHERE "{c}" IS NULL), min("{c}")::VARCHAR, max("{c}")::VARCHAR'
    for c in common)
rs = con.execute(f"SELECT {agg} FROM spark").fetchone()
rr = con.execute(f"SELECT {agg} FROM ray").fetchone()
for i, c in enumerate(common):
    s, r = rs[3 * i:3 * i + 3], rr[3 * i:3 * i + 3]
    same = s == r
    ok &= same
    print(f"    {c:24s} {'PASS' if same else 'FAIL'}" + ("" if same else f"  spark={s} ray={r}"))

# 4. Order-independent row hash over ALL common columns
print("[4] Whole-row checksum (order independent) ...", flush=True)
row = "concat_ws('|', " + ", ".join(f"coalesce(\"{c}\"::VARCHAR, '<NULL>')" for c in common) + ")"
q = f"SELECT count(*), sum(hash({row})::HUGEINT) FROM "
hs = con.execute(q + "spark").fetchone()
hr = con.execute(q + "ray").fetchone()
print(f"    spark={hs}\n    ray  ={hr}\n    ->", "PASS" if hs == hr else "FAIL")
ok &= hs == hr

# 5. If the checksum failed, show example differing rows
if hs != hr:
    cols = ", ".join(f'"{c}"' for c in common)
    for a, b in (("spark", "ray"), ("ray", "spark")):
        print(f"[5] Sample rows in {a} but not {b}:")
        for r in con.execute(f"SELECT {cols} FROM {a} EXCEPT ALL SELECT {cols} FROM {b} LIMIT 5").fetchall():
            print("   ", r)

print(f"\nOVERALL: {'PASS - outputs match exactly' if ok else 'FAIL - see above'}  ({time.time() - t0:.1f} s)")
