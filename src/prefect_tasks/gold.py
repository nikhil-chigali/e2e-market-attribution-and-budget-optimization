"""Prefect tasks for Gold layer - business-ready analytical tables."""

from pathlib import Path
import time

import duckdb
from prefect import task, get_run_logger

# Project root: src/prefect_tasks/gold.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

USER_JOURNEYS_PATH = _PROJECT_ROOT / "data" / "gold" / "user_journeys.parquet"


@task(name="create_user_journeys")
def create_user_journeys_table(silver_path: str) -> str:
    """
    Builds gold.user_journeys: one row per user with full journey and metrics.

    Groups by uid, computes sequences (campaign, click, time gaps), and
    journey-level metrics. Uses day_number for journey length and includes
    criteo_attributed flags, campaign and click sequences, time_gaps_seconds,
    and basic cost and conversion aggregates.

    Args:
        silver_path: Path to the silver clean_impressions parquet file.

    Returns:
        Path to the gold user_journeys parquet file.
    """
    logger = get_run_logger()
    silver_str = str(Path(silver_path).resolve()).replace("\\", "/")
    output_path = USER_JOURNEYS_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_str = str(output_path.resolve()).replace("\\", "/")

    logger.info(
        "Starting create_user_journeys_table",
        extra={"silver_path": silver_str, "user_journeys_path": output_str},
    )
    start = time.perf_counter()

    conn = duckdb.connect()

    # time_gaps: two-step CTE - (1) per-row gap, (2) STRING_AGG (LAG cannot sit inside STRING_AGG in one step)
    conn.execute(
        f"""
        COPY (
            WITH silver AS (
                SELECT impression_id, uid, timestamp, day_number, campaign, click, conversion,
                       conversion_timestamp, conversion_id, attribution, cost,
                       click_pos, click_nb
                FROM read_parquet('{silver_str}')
            ),
            user_stats AS (
                SELECT
                    uid,
                    MIN(day_number) AS first_touch_day,
                    MAX(day_number) AS last_touch_day,
                    MIN(timestamp) AS first_timestamp,
                    MAX(CASE WHEN conversion THEN conversion_timestamp END) AS conversion_timestamp,
                    COUNT(*) AS total_impressions,
                    SUM(CAST(click AS INTEGER)) AS total_clicks,
                    SUM(cost) AS total_cost,
                    MAX(CAST(conversion AS INTEGER)) AS converted,
                    MAX(CAST(attribution AS INTEGER)) AS criteo_attributed,
                    STRING_AGG(campaign, '|' ORDER BY timestamp) AS campaign_sequence,
                    STRING_AGG(CAST(CAST(attribution AS INTEGER) AS VARCHAR), '|' ORDER BY timestamp) AS attribution_sequence,
                    STRING_AGG(CAST(CAST(click AS INTEGER) AS VARCHAR), '|' ORDER BY timestamp) AS click_sequence,
                    STRING_AGG(DISTINCT conversion_id, '|') FILTER (WHERE conversion_id IS NOT NULL) AS conversion_ids,
                    COUNT(DISTINCT campaign) AS unique_campaigns,
                    FIRST(campaign ORDER BY timestamp) AS first_touch_campaign,
                    LAST(campaign ORDER BY timestamp) AS last_touch_campaign,
                    MAX(click_nb) AS total_clicks_before_conversion
                FROM silver
                GROUP BY uid
            ),
            gaps_per_row AS (
                SELECT
                    uid,
                    timestamp,
                    timestamp - LAG(timestamp) OVER (PARTITION BY uid ORDER BY timestamp) AS gap_seconds
                FROM silver
            ),
            time_gaps AS (
                SELECT
                    uid,
                    STRING_AGG(CAST(COALESCE(gap_seconds, 0) AS VARCHAR), '|' ORDER BY timestamp) AS time_gaps_seconds
                FROM gaps_per_row
                GROUP BY uid
            )
            SELECT
                u.uid,
                u.first_touch_day,
                u.last_touch_day,
                u.last_touch_day - u.first_touch_day AS journey_length_days,
                u.first_timestamp,
                u.conversion_timestamp,
                CASE
                    WHEN u.conversion_timestamp IS NOT NULL
                    THEN u.conversion_timestamp - u.first_timestamp
                    ELSE NULL
                END AS time_to_conversion_seconds,
                u.total_impressions,
                u.total_clicks,
                u.total_cost,
                CAST(u.converted AS BOOLEAN) AS converted,
                CAST(u.criteo_attributed AS BOOLEAN) AS criteo_attributed,
                u.total_clicks_before_conversion,
                u.campaign_sequence,
                u.click_sequence,
                t.time_gaps_seconds,
                u.conversion_ids,
                u.unique_campaigns,
                u.first_touch_campaign,
                u.last_touch_campaign,
                (u.unique_campaigns > 1) AS cross_channel_journey,
                CASE
                    WHEN u.total_impressions > 1
                    THEN (u.last_touch_day - u.first_touch_day) * 1.0 / (u.total_impressions - 1)
                    ELSE 0
                END AS avg_days_between_touches,
                CASE
                    WHEN u.criteo_attributed = 1 THEN u.total_cost
                    ELSE NULL
                END AS user_acquisition_cost
            FROM user_stats u
            LEFT JOIN time_gaps t ON u.uid = t.uid
        ) TO '{output_str}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """
    )

    elapsed = time.perf_counter() - start
    logger.info(
        "Completed create_user_journeys_table in %.2f seconds",
        elapsed,
        extra={"user_journeys_path": str(output_path)},
    )

    return str(output_path)
