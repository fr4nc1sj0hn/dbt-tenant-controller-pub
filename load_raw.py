import os
import subprocess
from pathlib import Path


# Small helper to run the psql script that loads raw CSVs.
#
# This intentionally stays minimal (PoC), but it supports the two common toggles:
# - DB_NAME: which database to connect to
# - RAW_SCHEMA / RAW_DATA_DIR: where to load data

DB_NAME = os.getenv("POSTGRES_DB", "analytics")
RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw_sample")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/data/raw_data")

SQL_FILE = Path("reload_and_validate_raw.sql")


def run_psql() -> None:
    cmd = [
        "psql",
        "-d",
        DB_NAME,
        "-v",
        f"raw_schema={RAW_SCHEMA}",
        "-f",
        str(SQL_FILE),
    ]
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    print("Loading raw data into Postgres...")
    run_psql()
    print("Raw load complete.")
