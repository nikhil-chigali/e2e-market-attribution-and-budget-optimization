"""Prefect tasks for Silver layer - data cleaning and validation."""

import json
from pathlib import Path
from typing import Any

import duckdb
from prefect import task

# Project root: src/prefect_tasks/silver.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DQ_REPORT_PATH = _PROJECT_ROOT / "outputs" / "data_quality_report.json"
SILVER_OUTPUT_PATH = _PROJECT_ROOT / "data" / "silver" / "clean_impressions.parquet"


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
        - distinct_users, distinct_campaigns: counts
        - total_conversions: sum of conversions
        - attributed_conversions: sum of attribution
        - distinct_conversion_ids: count distinct conversion_id where not '-1'
        - bad_attribution_records: count where attribution=1 AND cpo=-1
        - min_timestamp, max_timestamp, timestamp_duration
        - total_rows
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

    # Count distinct entities, totals, attribution, conversion_ids
    stats = conn.execute(
        f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT uid) as distinct_users,
            COUNT(DISTINCT campaign) as distinct_campaigns,
            COALESCE(SUM(conversion), 0)::BIGINT as total_conversions,
            COALESCE(SUM(attribution), 0)::BIGINT as attributed_conversions,
            COUNT(DISTINCT CASE WHEN conversion_id IS NOT NULL AND conversion_id != '-1' THEN conversion_id END) as distinct_conversion_ids,
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp
        FROM read_parquet('{path_str}')
        """
    ).fetchone()

    # Bad attribution records: attribution=1 but cpo=-1
    bad_attribution = conn.execute(
        f"""
        SELECT COUNT(*) FROM read_parquet('{path_str}')
        WHERE attribution = 1 AND cpo = -1
        """
    ).fetchone()[0]
    dq_report["bad_attribution_records"] = bad_attribution

    dq_report.update({
        "total_rows": stats[0],
        "distinct_users": stats[1],
        "distinct_campaigns": stats[2],
        "total_conversions": stats[3],
        "attributed_conversions": stats[4],
        "distinct_conversion_ids": stats[5],
        "min_timestamp": stats[6],
        "max_timestamp": stats[7],
    })
    dq_report["timestamp_duration"] = (stats[7] - stats[6]) if stats[6] is not None and stats[7] is not None else None

    if save_report:
        out_path = Path(report_path) if report_path else DQ_REPORT_PATH
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(dq_report, f, indent=2)

    return dq_report


@task(name="transform_to_silver")
def transform_to_silver_layer(bronze_path: str) -> str:
    """
    Transforms bronze data into silver clean_impressions parquet.

    Timestamp treated as relative (seconds from start): day_number, hour_of_day.
    Keeps timestamp, conversion_timestamp, conversion_id, attribution, click context.
    cpo split into cost_per_order_actual (only when attribution=1) and cost_per_order_predicted (all rows).

    Args:
        bronze_path: Path to the bronze parquet file.

    Returns:
        Path to the silver parquet file.
    """
    bronze_str = str(Path(bronze_path).resolve()).replace("\\", "/")
    output_path = SILVER_OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_str = str(output_path.resolve()).replace("\\", "/")

    conn = duckdb.connect()

    conn.execute(
        f"""
        COPY (
            SELECT DISTINCT
                impression_id,
                timestamp,
                (timestamp / 86400.0)::INTEGER as day_number,
                ((timestamp % 86400) / 3600)::INTEGER as hour_of_day,
                uid,
                campaign,
                CAST(click AS BOOLEAN) as click,
                CAST(conversion AS BOOLEAN) as conversion,
                NULLIF(conversion_timestamp, -1) as conversion_timestamp,
                NULLIF(conversion_id, '-1') as conversion_id,
                CAST(attribution AS BOOLEAN) as attribution,
                NULLIF(click_pos, -1) as click_pos,
                NULLIF(click_nb, -1) as click_nb,
                cost,
                CASE WHEN attribution = 1 THEN cpo ELSE NULL END as cost_per_order_actual,
                cpo as cost_per_order_predicted,
                NULLIF(time_since_last_click, -1) as time_since_last_click,
                CASE
                    WHEN ((timestamp % 86400) / 3600)::INTEGER BETWEEN 6 AND 11 THEN 'morning'
                    WHEN ((timestamp % 86400) / 3600)::INTEGER BETWEEN 12 AND 17 THEN 'afternoon'
                    WHEN ((timestamp % 86400) / 3600)::INTEGER BETWEEN 18 AND 22 THEN 'evening'
                    ELSE 'night'
                END as time_period,
                cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9
            FROM read_parquet('{bronze_str}')
            WHERE cost > 0
        ) TO '{output_str}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """
    )

    return str(output_path)
