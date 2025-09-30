import duckdb
import os
import logging
from pathlib import Path
import requests
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join("logs", "load.log"),
    filemode="a"
)
logger = logging.getLogger(__name__)

# Base URL for NYC Taxi Parquet files
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def download_parquet(cab: str, year: int, month: int, target_dir: Path) -> Path:
    """Downloads a monthly taxi trip parquet file if not already present."""
    filename = f"{cab}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    out_path = target_dir / filename

    if not out_path.exists():
        logger.info(f"Downloading {url}")
        try:
            resp = requests.get(url, stream=True, timeout=30)
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Saved {filename} to {out_path}")
            time.sleep(5)  # short pause to be safe
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            raise
    else:
        logger.info(f"File already exists: {out_path}")

    return out_path

def summarize_table(con, table: str):
    """Prints and logs basic summary stats for a DuckDB table."""
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        summary = {"table": table, "rows": row_count}

        if table in ("yellow", "green"):
            year_months = con.execute(f"""
                SELECT MIN(trip_year), MAX(trip_year),
                       MIN(trip_month), MAX(trip_month)
                FROM {table}
            """).fetchone()
            summary["year_range"] = f"{year_months[0]}–{year_months[1]}"
            summary["month_range"] = f"{year_months[2]}–{year_months[3]}"
            stats = con.execute(f"""
                SELECT
                    AVG(trip_distance),
                    MAX(trip_distance),
                    AVG(total_amount)
                FROM {table}
            """).fetchone()
            summary["avg_distance"] = round(stats[0] or 0, 2)
            summary["max_distance"] = round(stats[1] or 0, 2)
            summary["avg_total_amount"] = round(stats[2] or 0, 2)

        logger.info(f"Summary for {table}: {summary}")
        print(f"Summary for {table}: {summary}")
    except Exception as e:
        logger.error(f"Error summarizing {table}: {e}")
        print(f"Error summarizing {table}: {e}")

def load_parquet_files(years=[2024], cab_types=("yellow", "green")):
    """Download, load, and create DuckDB tables for NYC taxi data (2024 only)."""
    con = None
    try:
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB instance")

        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)

        for cab in cab_types:
            all_files = []
            total_downloads = len(years) * 12
            current_download = 0

            print(f"\nProcessing {cab} taxi data for 2024")
            print(f"Total files to process: {total_downloads}")

            for year in years:
                for month in range(1, 13):
                    current_download += 1
                    try:
                        print(f"Progress: {current_download}/{total_downloads} - Processing {cab} {year}-{month:02d}")
                        fpath = download_parquet(cab, year, month, data_dir)
                        all_files.append(str(fpath))
                    except Exception as e:
                        logger.warning(f"Could not download {cab} {year}-{month:02d}: {e}")
                        print(f"Warning: Could not download {cab} {year}-{month:02d}: {e}")

            if not all_files:
                logger.warning(f"No files found for {cab}, skipping.")
                continue

            print(f"\nLoading {len(all_files)} files into DuckDB for {cab} table...")
            table_name = cab
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *,
                       CAST(regexp_extract(filename, '_(\\d{{4}})-', 1) AS INTEGER) AS trip_year,
                       CAST(regexp_extract(filename, '-(\\d{{2}})\\.parquet', 1) AS INTEGER) AS trip_month
                FROM parquet_scan({all_files}, HIVE_PARTITIONING=0, FILENAME=1, union_by_name=True)
            """)
            logger.info(f"Created/loaded table {table_name}")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"{cab.capitalize()} trips loaded: {count:,} rows")
            print(f"{cab.capitalize()} trips loaded: {count:,} rows")

        # Load vehicle emissions CSV
        csv_path = data_dir / "vehicle_emissions.csv"
        if csv_path.exists():
            print(f"\nLoading vehicle emissions data...")
            con.execute(f"""
                CREATE OR REPLACE TABLE vehicle_emissions AS
                SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE)
            """)
            count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
            logger.info(f"Vehicle emissions loaded: {count:,} rows")
            print(f"Vehicle emissions loaded: {count:,} rows")
        else:
            logger.warning("vehicle_emissions.csv not found in data/")
            print("Warning: vehicle_emissions.csv not found in data/")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    print("=" * 60)
    print("STARTING DATA LOADING PROCESS")
    print("Loading NYC Taxi Data for 2024")
    print("=" * 60)

    load_parquet_files(years=[2024])

    print("\n" + "=" * 60)
    print("GENERATING DATA SUMMARIES")
    print("=" * 60)

    con = duckdb.connect(database='emissions.duckdb', read_only=True)
    for table in ["yellow", "green", "vehicle_emissions"]:
        try:
            summarize_table(con, table)
        except Exception as e:
            logger.warning(f"Could not summarize {table}: {e}")
            print(f"Warning: Could not summarize {table}: {e}")
    con.close()

    print("\nData loading finished")