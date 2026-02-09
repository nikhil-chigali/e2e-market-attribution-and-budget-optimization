"""Prefect tasks for Gold layer - business-ready analytical tables."""

from pathlib import Path

import duckdb
from prefect import task

# Project root: src/prefect_tasks/gold.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

USER_JOURNEYS_PATH = _PROJECT_ROOT / "data" / "gold" / "user_journeys.parquet"
CAMPAIGN_TOUCHPOINTS_PATH = _PROJECT_ROOT / "data" / "gold" / "campaign_touchpoints.parquet"


@task(name="create_user_journeys")
def create_user_journeys_table(silver_path: str) -> str:
    """
    Builds gold.user_journeys: one row per user with full journey and metrics.

    Groups by uid, computes sequences (campaign, click, time gaps), and
    journey-level metrics. Uses day_number for journey length; includes
    criteo_attributed, actual_cpo_sum, avg_predicted_cpo, conversion_ids,
    time_gaps_seconds, user_acquisition_cost.

    Args:
        silver_path: Path to the silver clean_impressions parquet file.

    Returns:
        Path to the gold user_journeys parquet file.
    """
    silver_str = str(Path(silver_path).resolve()).replace("\\", "/")
    output_path = USER_JOURNEYS_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_str = str(output_path.resolve()).replace("\\", "/")

    conn = duckdb.connect()

    # time_gaps: two-step CTE - (1) per-row gap, (2) STRING_AGG (LAG cannot sit inside STRING_AGG in one step)
    conn.execute(
        f"""
        COPY (
            WITH silver AS (
                SELECT impression_id, uid, timestamp, day_number, campaign, click, conversion,
                       conversion_timestamp, conversion_id, attribution, cost,
                       cost_per_order_actual, cost_per_order_predicted,
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
                    SUM(cost_per_order_actual) AS actual_cpo_sum,
                    AVG(cost_per_order_predicted) AS avg_predicted_cpo,
                    STRING_AGG(campaign, '|' ORDER BY timestamp) AS campaign_sequence,
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
                u.actual_cpo_sum,
                u.avg_predicted_cpo,
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

    return str(output_path)


@task(name="create_campaign_touchpoints")
def create_campaign_touchpoints_table(silver_path: str) -> str:
    """
    Builds gold.campaign_touchpoints: one row per impression enriched with journey context.

    Adds touchpoint_position, is_first_touch, is_last_touch, total_journey_length,
    days_since_first_touch, days_until_conversion, position_normalized,
    is_repeat_campaign, campaign_exposure_count, cross_channel_flag,
    user_has_criteo_attribution for attribution modeling.

    Args:
        silver_path: Path to the silver clean_impressions parquet file.

    Returns:
        Path to the gold campaign_touchpoints parquet file.
    """
    silver_str = str(Path(silver_path).resolve()).replace("\\", "/")
    output_path = CAMPAIGN_TOUCHPOINTS_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_str = str(output_path.resolve()).replace("\\", "/")

    conn = duckdb.connect()

    conn.execute(
        f"""
        COPY (
            WITH journey_context AS (
                SELECT
                    impression_id,
                    uid,
                    timestamp,
                    day_number,
                    hour_of_day,
                    campaign,
                    click,
                    conversion,
                    conversion_id,
                    attribution,
                    cost,
                    cost_per_order_actual,
                    cost_per_order_predicted,
                    click_pos,
                    click_nb,
                    time_since_last_click,
                    time_period,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY timestamp) AS touchpoint_position,
                    COUNT(*) OVER (PARTITION BY uid) AS total_journey_length,
                    MIN(timestamp) OVER (PARTITION BY uid) AS user_first_timestamp,
                    MAX(CASE WHEN conversion THEN conversion_timestamp END) OVER (PARTITION BY uid) AS user_conversion_timestamp,
                    MAX(CAST(attribution AS INTEGER)) OVER (PARTITION BY uid) AS user_attributed_by_criteo,
                    LAG(campaign) OVER (PARTITION BY uid ORDER BY timestamp) AS previous_campaign,
                    ROW_NUMBER() OVER (PARTITION BY uid, campaign ORDER BY timestamp) AS campaign_exposure_count
                FROM read_parquet('{silver_str}')
            )
            SELECT
                impression_id,
                uid,
                timestamp,
                day_number,
                hour_of_day,
                campaign,
                click,
                conversion,
                conversion_id,
                attribution,
                cost,
                cost_per_order_actual,
                cost_per_order_predicted,
                click_pos,
                click_nb,
                time_since_last_click,
                time_period,
                touchpoint_position,
                (touchpoint_position = 1) AS is_first_touch,
                (touchpoint_position = total_journey_length) AS is_last_touch,
                total_journey_length,
                (timestamp - user_first_timestamp) / 86400.0 AS days_since_first_touch,
                CASE
                    WHEN user_conversion_timestamp IS NOT NULL
                    THEN (user_conversion_timestamp - timestamp) / 86400.0
                    ELSE NULL
                END AS days_until_conversion,
                touchpoint_position * 1.0 / total_journey_length AS position_normalized,
                (campaign_exposure_count > 1) AS is_repeat_campaign,
                campaign_exposure_count,
                (previous_campaign IS NOT NULL AND previous_campaign != campaign) AS cross_channel_flag,
                CAST(user_attributed_by_criteo AS BOOLEAN) AS user_has_criteo_attribution,
                (click AND conversion) AS is_converting_click,
                (click_pos = 0) AS is_first_click_in_sequence
            FROM journey_context
        ) TO '{output_str}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """
    )

    return str(output_path)
