"""
Criteo attribution ETL pipeline.

Bronze: Kaggle download -> Raw Parquet
Silver: Clean & validate -> Clean Parquet
Gold: Feature engineering -> Business-ready tables
"""

import sys
from pathlib import Path

from prefect import flow

# Add project root to path for src imports when running as script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.prefect_tasks.bronze import load_to_bronze_layer
from src.prefect_tasks.download import download_criteo_from_kaggle
from src.prefect_tasks.silver import validate_bronze_data


@flow(name="criteo_etl_pipeline")
def criteo_attribution_etl() -> dict:
    """
    End-to-end pipeline from Kaggle to Gold parquet files.

    Returns:
        Dict with paths to outputs and data quality report.
    """
    # Bronze Layer
    raw_data_path = download_criteo_from_kaggle()
    bronze_path = load_to_bronze_layer(raw_data_path=raw_data_path)

    # Silver Layer
    dq_report = validate_bronze_data(bronze_path=bronze_path)
    # silver_path = transform_to_silver_layer(bronze_path=bronze_path)

    # Gold Layer (placeholder - to be added)
    # user_journeys_path = create_user_journeys_table(silver_path=silver_path)
    # touchpoints_path = create_campaign_touchpoints_table(silver_path=silver_path)

    return {
        "raw_data_path": raw_data_path,
        "bronze_path": bronze_path,
        "data_quality": dq_report,
    }


if __name__ == "__main__":
    result = criteo_attribution_etl()
    print("Pipeline complete:", result)
