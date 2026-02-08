"""Prefect tasks for Bronze layer - raw data ingestion."""

from pathlib import Path

import duckdb
from prefect import task

# Project root: src/prefect_tasks/bronze.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

BRONZE_OUTPUT_PATH = _PROJECT_ROOT / "data" / "bronze" / "raw_impressions.parquet"


def _find_data_file(raw_data_dir: str | Path) -> Path:
    """
    Find the main data file (TSV or CSV) within the downloaded dataset directory.

    kagglehub may return a path that includes nested subdirectories.
    If the path points directly to a TSV/CSV file, returns it as-is.
    """
    base = Path(raw_data_dir)
    if base.is_file() and base.suffix.lower() in (".tsv", ".csv"):
        return base
    if not base.is_dir():
        raise FileNotFoundError(f"Raw data path is not a directory or file: {raw_data_dir}")

    # Search for TSV or CSV files (Criteo dataset is tab-separated)
    for pattern in ("*.tsv", "*.csv"):
        matches = list(base.rglob(pattern))
        if matches:
            # Prefer files in the root of the returned path, then by size (largest first)
            matches.sort(key=lambda p: (len(p.relative_to(base).parts), -p.stat().st_size))
            return matches[0]

    raise FileNotFoundError(f"No TSV or CSV file found in {raw_data_dir}")


@task(name="load_to_bronze")
def load_to_bronze_layer(raw_data_path: str) -> str:
    """
    Loads raw Criteo data into Bronze Parquet using DuckDB.

    Args:
        raw_data_path: Path to the downloaded dataset directory (from kagglehub).

    Returns:
        Path to the bronze parquet file.

    Schema:
        - impression_id (auto-increment via ROW_NUMBER())
        - All source columns: timestamp, uid, campaign, click, conversion, cost, etc.
    """
    data_file = _find_data_file(raw_data_path)
    output_path = BRONZE_OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    # Use explicit paths - DuckDB parameter binding can swap COPY TO vs read_csv
    data_file_str = str(data_file.resolve()).replace("\\", "/")
    output_path_str = str(output_path.resolve()).replace("\\", "/")

    # Criteo dataset is tab-separated
    conn.execute(
        f"""
        COPY (
            SELECT
                ROW_NUMBER() OVER () as impression_id,
                *
            FROM read_csv_auto('{data_file_str}', delim=E'\\t')
        ) TO '{output_path_str}' (FORMAT PARQUET)
        """
    )

    return str(output_path)

if __name__ == "__main__":
    load_to_bronze_layer(raw_data_path="data/raw/criteo_attribution_modeling.tsv")
    print("Bronze layer loaded successfully")