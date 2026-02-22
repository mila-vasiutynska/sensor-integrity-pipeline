import duckdb

def show(path_glob: str, limit: int = 10) -> None:
    print(f"\n--- {path_glob} ---")
    q = f"SELECT * FROM '{path_glob}' LIMIT {limit}"
    print(duckdb.sql(q).df())

def stats(path_glob: str) -> None:
    q = f"""
    SELECT
      COUNT(*) AS rows,
      MIN(event_ts) AS min_ts,
      MAX(event_ts) AS max_ts,
      COUNT(DISTINCT serialNumber) AS devices
    FROM '{path_glob}'
    """
    print(duckdb.sql(q).df())

if __name__ == "__main__":
    show("out/curated/*.parquet", 10)
    stats("out/curated/*.parquet")