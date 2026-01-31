from pathlib import Path
import duckdb

DATA_DIR = Path("data/staging")
DB_PATH = Path("data/vr_trains.duckdb")

def sql_str(p: Path) -> str:
    # DuckDB SQL string literal. Tuplaa yksittäiset lainausmerkit.
    return str(p).replace("\\", "/").replace("'", "''")

def merge_live_trains(conn: duckdb.DuckDBPyConnection, json_path: Path) -> None:
    conn.execute("create schema if not exists bronze")

    path = sql_str(json_path)

    conn.execute(f"""
        create or replace temp view v_live_trains as
        select * from read_json_auto('{path}')
    """)

    conn.execute("""
        create table if not exists bronze.live_trains as
        select * from v_live_trains limit 0
    """)

    conn.execute("""
        insert into bronze.live_trains
        by name
        select v.*
        from v_live_trains v
        where not exists (
          select 1
          from bronze.live_trains t
          where t.trainNumber   = v.trainNumber
            and t.departureDate = v.departureDate
            and t.version       = v.version
        )
    """)

def merge_train_locations(conn: duckdb.DuckDBPyConnection, json_path: Path) -> None:
    conn.execute("create schema if not exists bronze")

    if json_path.stat().st_size == 0:
        print(f"Skipping empty train locations file: {json_path}")
        return

    raw_payload = json_path.read_text(encoding="utf-8").strip()
    if raw_payload in {"", "[]"}:
        print(f"Skipping empty train locations payload: {json_path}")
        return

    path = sql_str(json_path)

    conn.execute(f"""
        create or replace temp view v_train_locations_raw as
        select *
        from read_json_auto('{path}')
    """)

    columns = conn.execute("pragma_table_info('v_train_locations_raw')").fetchall()
    if not columns:
        print(f"Skipping train locations with no columns: {json_path}")
        return

    conn.execute("""
        create or replace temp view v_train_locations as
        select
          *,
          md5(cast(to_json(struct_pack(*)) as varchar)) as _row_hash
        from v_train_locations_raw
    """)

    conn.execute("""
        create table if not exists bronze.train_locations as
        select * from v_train_locations limit 0
    """)

    conn.execute("""
        insert into bronze.train_locations
        by name
        select v.*
        from v_train_locations v
        where not exists (
          select 1
          from bronze.train_locations t
          where t.trainNumber   = v.trainNumber
            and t.departureDate = v.departureDate
            and t._row_hash     = v._row_hash
        )
    """)

def main() -> None:
    live_trains_path = DATA_DIR / "live_trains.json"
    train_locations_path = DATA_DIR / "train_locations.json"

    if not live_trains_path.exists():
        raise FileNotFoundError(f"Missing {live_trains_path}")

    if not train_locations_path.exists():
        raise FileNotFoundError(f"Missing {train_locations_path}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))

    # Perusasetuksia, jotta saat toistettavat ajot
    conn.execute("pragma threads=4")

    merge_live_trains(conn, live_trains_path)
    merge_train_locations(conn, train_locations_path)

    conn.close()
    print("Bronze merge complete")


if __name__ == "__main__":
    main()
