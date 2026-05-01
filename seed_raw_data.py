import pandas as pd
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

# -----------------------
# Config
# -----------------------
# Number of rows to generate for the raw fact table (raw_transactions)
NUM_TRANSACTION_ROWS = 1000
OUTPUT_DIR = Path("raw_data")

# Keep the output deterministic for demos/tests
random.seed(42)
OUTPUT_DIR.mkdir(exist_ok=True)

# Use current runtime timestamps (UTC) truncated to seconds for stable CSV output
_NOW_UTC = datetime.now(timezone.utc).replace(microsecond=0)

# Ingestion is "now"; transaction timestamp is set to "now" as well.
DIM_INGESTION_TIMESTAMP = _NOW_UTC
TX_TIMESTAMP = _NOW_UTC

SOURCE_SYSTEM = "synthetic"
CSV_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# -----------------------
# Dimension Definitions (counts inferred from the existing repo sample)
# -----------------------
DIM_COUNTS: dict[str, int] = {
    "dim1": 4,
    "dim2": 5,
    "dim3": 6,
    "dim4": 3,
    "dim5": 7,
    "dim6": 5,
    "dim7": 3,
    "dim8": 5,
    "dim9": 4,
}


def generate_dimension(dim_name: str, count: int) -> pd.DataFrame:
    """Generate a flat dimension table.

    Columns:
    - <dimN_code>
    - <dimN_label>
    - sort_order
    - is_active
    - source_system
    - ingestion_timestamp

    Notes:
    - The label format is inferred from the provided dim1 sample ("Dim1 Val2").
    - Also mimics the sample ordering where code2..codeN are emitted first and
      code1 is emitted last with an "Updated" label.
    """

    dim_number = dim_name.replace("dim", "")

    def label_for(i: int) -> str:
        if i == 1:
            return f"Dim{dim_number} Val{i} Updated"
        return f"Dim{dim_number} Val{i}"

    rows: list[dict] = []

    # Emit code2..codeN first, then code1 last (to mirror the sample ordering)
    for i in range(2, count + 1):
        rows.append(
            {
                f"{dim_name}_code": f"{dim_name}_code{i}",
                f"{dim_name}_label": label_for(i),
                "sort_order": i,
                "is_active": True,
                "source_system": SOURCE_SYSTEM,
                "ingestion_timestamp": DIM_INGESTION_TIMESTAMP,
            }
        )

    rows.append(
        {
            f"{dim_name}_code": f"{dim_name}_code1",
            f"{dim_name}_label": label_for(1),
            "sort_order": 1,
            "is_active": True,
            "source_system": SOURCE_SYSTEM,
            "ingestion_timestamp": DIM_INGESTION_TIMESTAMP,
        }
    )

    return pd.DataFrame(rows)


def _measure(low: float, high: float) -> float:
    return round(random.uniform(low, high), 2)


def generate_transactions(dim_to_codes: dict[str, list[str]]) -> pd.DataFrame:
    """Generate raw fact rows for `raw_transactions`.

    Columns follow `seed_raw_schema.sql`:
    record_id, transaction_id, dim1_code..dim9_code, measure1..measure5,
    transaction_timestamp, source_system, ingestion_timestamp
    """

    # Create deterministic grouping so we naturally get repeated record_ids
    # with multiple transaction_ids.
    transactions_per_record = 5
    num_records = max(1, NUM_TRANSACTION_ROWS // transactions_per_record)

    rows: list[dict] = []
    for record_n in range(1, num_records + 1):
        record_id = f"R{record_n:04d}"
        for tx_n in range(1, transactions_per_record + 1):
            transaction_id = f"T{tx_n:02d}"

            row = {
                "record_id": record_id,
                "transaction_id": transaction_id,
                # dim1_code..dim9_code
                **{f"{dim}_code": random.choice(codes) for dim, codes in dim_to_codes.items()},
                # measures (ranges chosen to look similar to the sample)
                "measure1": _measure(50, 150),
                "measure2": _measure(20, 100),
                "measure3": _measure(80, 250),
                "measure4": _measure(250, 750),
                "measure5": _measure(400, 1500),
                "transaction_timestamp": TX_TIMESTAMP,
                "source_system": SOURCE_SYSTEM,
                "ingestion_timestamp": TX_TIMESTAMP,
            }
            rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure stable column order (matches seed_raw_schema.sql)
    dim_cols = [f"dim{i}_code" for i in range(1, 10)]
    ordered_cols = (
        ["record_id", "transaction_id"]
        + dim_cols
        + [
            "measure1",
            "measure2",
            "measure3",
            "measure4",
            "measure5",
            "transaction_timestamp",
            "source_system",
            "ingestion_timestamp",
        ]
    )
    # Pandas returns a DataFrame when slicing with a list of columns, but some
    # type checkers infer overly-broad unions; keep the return type explicit.
    return cast(pd.DataFrame, df.loc[:, ordered_cols])


def main() -> None:
    # 1) Dimensions
    dim_to_codes: dict[str, list[str]] = {}
    for dim_name, count in DIM_COUNTS.items():
        dim_df = generate_dimension(dim_name, count)
        dim_df.to_csv(
            OUTPUT_DIR / f"raw_{dim_name}.csv",
            index=False,
            date_format=CSV_DATETIME_FORMAT,
        )
        dim_to_codes[dim_name] = dim_df[f"{dim_name}_code"].tolist()

    # 2) Fact transactions
    tx_df = generate_transactions(dim_to_codes)
    tx_df.to_csv(
        OUTPUT_DIR / "raw_transactions.csv",
        index=False,
        date_format=CSV_DATETIME_FORMAT,
    )


if __name__ == "__main__":
    main()
