"""Prefect tasks for Silver layer - data cleaning and validation."""

import json
from pathlib import Path
from typing import Any

import duckdb
from prefect import task

# Project root: src/prefect_tasks/silver.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DQ_REPORT_PATH = _PROJECT_ROOT / "outputs" / "data_quality_report.json"


@task(name="validate_bronze_data")
def validate_bronze_data(
    bronze_path: str,
    save_report: bool = True,
    report_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Runs data quality checks on bronze layer and returns a report.

    Args:
        bronze_path: Path to the bronze parquet file.
        save_report: Whether to save the report to JSON. Default True.
        report_path: Path for the report file. Defaults to outputs/data_quality_report.json.

    Returns:
        Dict with DQ metrics:
        - null_uids: count of rows with null uid
        - negative_costs: count of rows with cost < 0
        - conversion_without_click: count of conversions without a prior click (DQ issue)
        - distinct_users: count of distinct uids
        - distinct_campaigns: count of distinct campaigns
        - total_conversions: sum of conversions
        - total_rows: total row count
        - timestamp_min, timestamp_max: timestamp range
        - null_uids_pct: percentage of null uids (0% expected)
    """
    path_str = str(Path(bronze_path).resolve()).replace("\\", "/")

    conn = duckdb.connect()
    dq_report: dict[str, Any] = {}

    # Check null uids
    null_uids = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path_str}') WHERE uid IS NULL"
    ).fetchone()[0]
    dq_report["null_uids"] = null_uids

    # Check negative costs
    negative_costs = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path_str}') WHERE cost < 0"
    ).fetchone()[0]
    dq_report["negative_costs"] = negative_costs

    # Check conversion without click (data quality issue - conversion=1 but click=0 on same row)
    conversion_without_click = conn.execute(
        f"""
        SELECT COUNT(*) FROM read_parquet('{path_str}')
        WHERE conversion = 1 AND click = 0
        """
    ).fetchone()[0]
    dq_report["conversion_without_click"] = conversion_without_click

    # Count distinct entities and totals
    stats = conn.execute(
        f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT uid) as distinct_users,
            COUNT(DISTINCT campaign) as distinct_campaigns,
            COALESCE(SUM(conversion), 0)::BIGINT as total_conversions,
            MIN(timestamp) as timestamp_min,
            MAX(timestamp) as timestamp_max
        FROM read_parquet('{path_str}')
        """
    ).fetchone()

    dq_report.update({
        "total_rows": stats[0],
        "distinct_users": stats[1],
        "distinct_campaigns": stats[2],
        "total_conversions": stats[3],
        "timestamp_min": stats[4],
        "timestamp_max": stats[5],
    })

    # Derived metrics
    total_rows = stats[0]
    dq_report["null_uids_pct"] = round(100.0 * null_uids / total_rows, 4) if total_rows else 0
    dq_report["conversion_without_click_pct"] = (
        round(100.0 * conversion_without_click / total_rows, 4) if total_rows else 0
    )

    if save_report:
        out_path = Path(report_path) if report_path else DQ_REPORT_PATH
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(dq_report, f, indent=2)

    return dq_report
