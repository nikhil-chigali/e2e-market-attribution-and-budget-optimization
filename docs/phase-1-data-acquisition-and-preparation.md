# Phase 1: Data Acquisition and Preparation

This document describes the Phase 1 pipeline for the Criteo Attribution Modeling dataset: data fields, operations, and checks. It serves as the reference for schema, transformations, and data quality. Phase 2 will introduce multi-touch attribution modeling and validation. Field descriptions are based on the [Kaggle dataset page](https://www.kaggle.com/datasets/sharatsachin/criteo-attribution-modeling).

**Contents:** [Data Fields](#data-fields) · [Pipeline Operations](#pipeline-operations-performed) · [Data Flow](#data-flow-summary)

---

## Dataset Overview

The dataset represents a sample of **30 days** of Criteo live traffic data. Each row corresponds to **one impression** (a banner display) shown to a user. The data has been sub-sampled and anonymized. Files are **tab-separated (TSV)**.

**Key figures (from Kaggle):**
- ~2.4 GB uncompressed
- ~16.5M impressions
- ~45K conversions
- ~700 campaigns

**How to run the pipeline:** Execute `python prefect_flows/criteo_etl.py` from the project root. Prerequisites: Kaggle credentials configured for [kagglehub](https://github.com/Kaggle/kagglehub) (e.g., `~/.kaggle/kaggle.json` or `KAGGLE_USERNAME`/`KAGGLE_KEY` environment variables). Logs are written to console and `outputs/etl.log`. Task timings are logged for profiling.

---

## Data Fields

| Field | Type | Description |
|-------|------|--------------|
| **timestamp** | INTEGER | **Relative** time only: seconds from start (starting from 0 for the first impression). Not Unix epoch. The dataset is sorted by timestamp. |
| **uid** | TEXT | A unique user identifier (anonymized). |
| **campaign** | TEXT | A unique identifier for the campaign. |
| **conversion** | INTEGER (0/1) | 1 if there was a conversion in the 30 days after the impression (independent of whether this impression was last-click or not); 0 otherwise. |
| **conversion_timestamp** | INTEGER | Timestamp of the conversion, or **-1** if no conversion was observed. |
| **conversion_id** | TEXT | Unique identifier for each conversion (for journey reconstruction). **'-1'** if there was no conversion. |
| **attribution** | INTEGER (0/1) | 1 if the conversion was attributed to Criteo (ground truth for Phase 2 validation), 0 otherwise. |
| **click** | INTEGER (0/1) | 1 if the impression was clicked, 0 otherwise. |
| **click_pos** | INTEGER | Position of the click before a conversion (0 for first-click). **-1** when not a conversion; only populated for conversions. |
| **click_nb** | INTEGER | Total clicks in journey before conversion. **-1** when not a conversion; only populated for conversions. |
| **cost** | REAL | Price paid by Criteo for this display. *Disclaimer: transformed value, not the real price.* |
| **cpo** | REAL | Cost-per-order (present in source; **not used in Phase 1 transformations**). *Disclaimer: transformed value, not the real price.* |
| **time_since_last_click** | INTEGER | Time since the last click (in seconds) for the given impression. **-1** if N/A. |
| **cat1** … **cat9** | TEXT | Contextual categorical features associated with the display. Meaning is undisclosed; used for click/conversion modeling. |

---

## Pipeline Operations Performed

### 1. Download (Kaggle → Raw)

**Task:** `download_criteo_from_kaggle`  
**Location:** `src/prefect_tasks/download.py`  
**Output:** `data/raw/` (dataset directory from kagglehub)

| Operation | Description |
|-----------|-------------|
| Download from Kaggle | Uses `kagglehub` to download the dataset `sharatsachin/criteo-attribution-modeling`. |
| Retries | 2 retries with 30-second delay on failure. |
| Storage | Files are written to `data/raw/` (or a custom `output_dir`). |

---

### 2. Bronze Layer (Raw TSV → Parquet)

**Task:** `load_to_bronze_layer`  
**Location:** `src/prefect_tasks/bronze.py`  
**Input:** Raw TSV from `data/raw/`  
**Output:** `data/bronze/raw_impressions.parquet`

| Operation | Description |
|-----------|-------------|
| File discovery | Locates the main TSV/CSV file in the downloaded directory (handles nested paths). |
| Format conversion | Reads tab-separated TSV with `delim='\t'` and writes Parquet. |
| Add impression_id | Adds `impression_id` via `ROW_NUMBER()` as a unique row identifier. |
| Schema preservation | Keeps all source columns as-is (no transformations). |
| Compression | Writes Parquet with **zstd** compression. |

**Bronze schema:** `impression_id` (INTEGER) + all source columns above.

---

### 3. Data Quality Validation (Bronze)

**Task:** `validate_bronze_data`  
**Location:** `src/prefect_tasks/silver.py`  
**Input:** `data/bronze/raw_impressions.parquet`  
**Output:** `outputs/data_quality_report.json`

| Check | Description | Expected |
|-------|-------------|----------|
| **Null uid** | Count of rows where `uid` IS NULL | 0% |
| **Negative costs** | Count of rows where `cost < 0` | 0 |
| **Total impressions** | `COUNT(*)` | — |
| **Distinct users** | `COUNT(DISTINCT uid)` | — |
| **Distinct campaigns** | `COUNT(DISTINCT campaign)` | — |
| **Total conversions** | `SUM(conversion)` | — |
| **Attributed conversions** | `SUM(attribution)` | — |
| **Distinct conversion_ids** | `COUNT(DISTINCT conversion_id)` where `conversion_id != '-1'` | — |
| **Timestamp range** | `min_timestamp`, `max_timestamp`, `max_campaign_duration` (max − min in seconds) | Within campaign period |

The report is written to `outputs/data_quality_report.json` as JSON.

---

### 4. Silver Layer (Bronze → Clean Impressions)

**Task:** `transform_to_silver_layer`  
**Location:** `src/prefect_tasks/silver.py`  
**Input:** `data/bronze/raw_impressions.parquet`  
**Output:** `data/silver/clean_impressions.parquet`

Creates a cleaned, analysis-ready table. Timestamp is treated as **relative only** (seconds from start); no calendar date. **Phase 1 does not derive any CPO-based columns** from the raw `cpo` field.

| Step | Operation | Reasoning |
|------|-----------|-----------|
| **1. Timestamp (relative)** | Keep `timestamp` (INTEGER); add `day_number = (timestamp / 86400)::INT`, `hour_of_day = ((timestamp % 86400) / 3600)::INT` | Timestamp is not Unix epoch; relative seconds enable day/hour buckets. |
| **2. Deduplication** | `SELECT DISTINCT` on the full row | Removes exact duplicate impressions. |
| **3. Type casting** | Cast `click`, `conversion`, `attribution` to BOOLEAN | Clear types for downstream logic. |
| **4. Sentinels** | `NULLIF(conversion_timestamp, -1)`, `NULLIF(conversion_id, '-1')`, `NULLIF(click_pos, -1)`, `NULLIF(click_nb, -1)`, `NULLIF(time_since_last_click, -1)` | -1 and '-1' become NULL. |
| **5. time_period** | CASE on `hour_of_day` → 'morning', 'afternoon', 'evening', 'night' | Morning (6–11), afternoon (12–17), evening (18–22), night (else). |
| **6. Filter** | `WHERE cost > 0` | Drops invalid or zero-cost rows. |
| **7. Column selection** | Keep `impression_id`, `timestamp`, `day_number`, `hour_of_day`, `uid`, `campaign`, `click`, `conversion`, `conversion_timestamp`, `conversion_id`, `attribution`, `click_pos`, `click_nb`, `cost`, `time_since_last_click`, `time_period`, `cat1`–`cat9` | Full click context and attribution (ground truth) for Phase 2. |

**Silver schema:** `impression_id`, `timestamp`, `day_number`, `hour_of_day`, `uid`, `campaign`, `click`, `conversion`, `conversion_timestamp`, `conversion_id`, `attribution`, `click_pos`, `click_nb`, `cost`, `time_since_last_click`, `time_period`, `cat1`–`cat9`.

---

### 5. Gold Layer: user_journeys

**Task:** `create_user_journeys_table`  
**Location:** `src/prefect_tasks/gold.py`  
**Input:** `data/silver/clean_impressions.parquet`  
**Output:** `data/gold/user_journeys.parquet`

One row per user with full journey sequence and journey-level metrics. Uses **day_number** (not calendar date).

| Operation | Description |
|-----------|-------------|
| Group by uid | Aggregates all impressions per user. |
| Day-based range | `first_touch_day`, `last_touch_day`, `journey_length_days = last_touch_day - first_touch_day`. |
| Timestamps | `first_timestamp`, `conversion_timestamp`, `time_to_conversion_seconds` (when converted). |
| Metrics | `total_impressions`, `total_clicks`, `total_cost`, `converted`, **criteo_attributed** (MAX(attribution)), **total_clicks_before_conversion** (MAX(click_nb)). |
| Sequences | `campaign_sequence`, `click_sequence` (ordered by timestamp), **time_gaps_seconds** (gaps between consecutive touches in seconds), **conversion_ids** (pipe-separated distinct conversion_id). |
| Context | `first_touch_campaign`, `last_touch_campaign`, `unique_campaigns`, `cross_channel_journey`, `avg_days_between_touches`, **user_acquisition_cost** (total_cost when criteo_attributed). |

**Gold user_journeys schema:**

| Column | Type | Description |
|--------|------|-------------|
| `uid` | TEXT | Primary key. |
| `first_touch_day` | INTEGER | `MIN(day_number)` per user. |
| `last_touch_day` | INTEGER | `MAX(day_number)` per user. |
| `journey_length_days` | INTEGER | `last_touch_day - first_touch_day`. |
| `first_timestamp` | INTEGER | First impression timestamp. |
| `conversion_timestamp` | INTEGER | When conversion happened (NULL if none). |
| `time_to_conversion_seconds` | INTEGER | `conversion_timestamp - first_timestamp` when converted. |
| `total_impressions` | INTEGER | `COUNT(*)` per user. |
| `total_clicks` | INTEGER | `SUM(click)` per user. |
| `total_cost` | REAL | `SUM(cost)` per user. |
| `converted` | BOOLEAN | `MAX(conversion)` per user. |
| `criteo_attributed` | BOOLEAN | Ground truth: did Criteo get attribution? |
| `total_clicks_before_conversion` | INTEGER | `MAX(click_nb)` per user (NULL if no conversion). |
| `campaign_sequence` | TEXT | Campaigns in order (by timestamp), pipe-separated. |
| `click_sequence` | TEXT | Click (0/1) per touch, pipe-separated. |
| `time_gaps_seconds` | TEXT | Gaps between consecutive touches (seconds), pipe-separated. |
| `conversion_ids` | TEXT | Distinct conversion_id values, pipe-separated. |
| `unique_campaigns` | INTEGER | `COUNT(DISTINCT campaign)` per user. |
| `first_touch_campaign` | TEXT | Campaign of earliest impression. |
| `last_touch_campaign` | TEXT | Campaign of latest impression. |
| `cross_channel_journey` | BOOLEAN | True if user saw more than one campaign. |
| `avg_days_between_touches` | REAL | (last_touch_day - first_touch_day) / (total_impressions - 1). |
| `user_acquisition_cost` | REAL | Total cost when criteo_attributed=1, NULL otherwise. |

Output uses **zstd** compression.

---

## Data Flow Summary

```
Kaggle (TSV)
     ↓  download_criteo_from_kaggle
data/raw/
     ↓  load_to_bronze_layer
data/bronze/raw_impressions.parquet
     ↓  validate_bronze_data          transform_to_silver_layer
outputs/data_quality_report.json     data/silver/clean_impressions.parquet
                                                ↓  create_user_journeys_table
                                     data/gold/user_journeys.parquet
```

---

## Related Files

| File | Purpose |
|------|---------|
| `prefect_flows/criteo_etl.py` | Main ETL pipeline (download → bronze → silver → gold) |
| `src/prefect_tasks/download.py` | Kaggle download task |
| `src/prefect_tasks/bronze.py` | Bronze layer ingestion |
| `src/prefect_tasks/silver.py` | Silver DQ and transformation |
| `src/prefect_tasks/gold.py` | Gold user_journeys table |
| `src/utils/logging_config.py` | File logging configuration |

---

## Citation

When using this dataset, cite:

> Eustache Diemert, Julien Meynet, Pierre Galland, Damien Lefortier. "Attribution Modeling Increases Efficiency of Bidding in Display Advertising." AdKDD & TargetAd Workshop, KDD 2017, Halifax, NS, Canada.
