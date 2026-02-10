"""
Criteo attribution ETL pipeline.

Bronze: Kaggle download -> Raw Parquet
Silver: Clean & validate -> Clean Parquet
Gold: Feature engineering -> Business-ready tables
"""

import sys
import time
from pathlib import Path

from prefect import flow, get_run_logger

# Add project root to path for src imports when running as script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.logging_config import configure_file_logging
from src.prefect_tasks.bronze import load_to_bronze_layer
from src.prefect_tasks.download import download_criteo_from_kaggle
from src.prefect_tasks.gold import create_user_journeys_table
from src.prefect_tasks.silver import transform_to_silver_layer, validate_bronze_data


@flow(name="criteo_etl_pipeline")
def criteo_attribution_etl() -> dict:
    """
    End-to-end pipeline from Kaggle to Gold parquet files.

    Returns:
        Dict with paths to outputs and data quality report.
    """
    logger = get_run_logger()
    timings: dict[str, float] = {}

    # Bronze Layer
    start = time.perf_counter()
    raw_data_path = download_criteo_from_kaggle()
    timings["download_criteo_from_kaggle"] = time.perf_counter() - start
    logger.info(
        "download_criteo_from_kaggle completed in %.2f seconds",
        timings["download_criteo_from_kaggle"],
        extra={"raw_data_path": raw_data_path},
    )

    start = time.perf_counter()
    bronze_path = load_to_bronze_layer(raw_data_path=raw_data_path)
    timings["load_to_bronze_layer"] = time.perf_counter() - start
    logger.info(
        "load_to_bronze_layer completed in %.2f seconds",
        timings["load_to_bronze_layer"],
        extra={"bronze_path": bronze_path},
    )

    # Silver Layer
    start = time.perf_counter()
    dq_report = validate_bronze_data(bronze_path=bronze_path)
    timings["validate_bronze_data"] = time.perf_counter() - start
    logger.info(
        "validate_bronze_data completed in %.2f seconds",
        timings["validate_bronze_data"],
        extra={"dq_keys": list(dq_report.keys())},
    )

    start = time.perf_counter()
    silver_path = transform_to_silver_layer(bronze_path=bronze_path)
    timings["transform_to_silver_layer"] = time.perf_counter() - start
    logger.info(
        "transform_to_silver_layer completed in %.2f seconds",
        timings["transform_to_silver_layer"],
        extra={"silver_path": silver_path},
    )

    # Gold Layer
    start = time.perf_counter()
    user_journeys_path = create_user_journeys_table(silver_path=silver_path)
    timings["create_user_journeys_table"] = time.perf_counter() - start
    logger.info(
        "create_user_journeys_table completed in %.2f seconds",
        timings["create_user_journeys_table"],
        extra={"user_journeys_path": user_journeys_path},
    )

    logger.info(
        "Pipeline step timings (seconds)",
        extra={"timings": timings},
    )

    return {
        "raw_data_path": raw_data_path,
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "user_journeys_path": user_journeys_path,
        "data_quality": dq_report,
        "timings": timings,
    }


if __name__ == "__main__":
    _project_root = Path(__file__).resolve().parent.parent
    _log_path = configure_file_logging(_project_root / "outputs")
    print(f"Logging to file: {_log_path}")

    result = criteo_attribution_etl()
    print("Pipeline complete:", result)
    dq = result.get("data_quality", {})
    total_conv = dq.get("total_conversions", 0) or 0
    attr_conv = dq.get("attributed_conversions", 0) or 0
    if total_conv > 0:
        print(
            f"Attributed conversions: {attr_conv:,} / {total_conv:,} ({attr_conv / total_conv:.2%})"
        )
