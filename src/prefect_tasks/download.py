"""Prefect tasks for downloading raw data from Kaggle."""

from pathlib import Path

import kagglehub
from prefect import task

# Project root: src/prefect_tasks/download.py -> project root is 3 parents up
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

CRITEO_DATASET_HANDLE = "sharatsachin/criteo-attribution-modeling"


@task(name="download_criteo_data", retries=2, retry_delay_seconds=30)
def download_criteo_from_kaggle(
    dataset_handle: str = CRITEO_DATASET_HANDLE,
    output_dir: Path | str | None = None,
) -> str:
    """
    Downloads Criteo attribution dataset from Kaggle using kagglehub.

    Args:
        dataset_handle: Kaggle dataset handle (e.g., 'owner/dataset-name').
        output_dir: Directory to store downloaded files. Defaults to project_root/data/raw.

    Returns:
        Path to the downloaded dataset directory (contains TSV/CSV files).
    """
    if output_dir is None:
        output_dir = _PROJECT_ROOT / "data" / "raw"
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    downloaded_path = kagglehub.dataset_download(
        handle=dataset_handle,
        output_dir=str(output_path),
    )
    return str(downloaded_path)
