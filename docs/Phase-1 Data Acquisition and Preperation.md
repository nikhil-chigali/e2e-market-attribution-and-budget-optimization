# Data Acquisition and Preperation
# **Phase 1 Architecture: Prefect Pipeline with Medallion Layers**

## **Pipeline Overview**

```
Kaggle API → Bronze (Raw) → Silver (Cleaned) → Gold (Business-Ready)
      ↓              ↓              ↓                   ↓  
  Local CSV    Parquet File    Parquet File        Parquet File
```
---
# **Step 1: Bronze Layer - Data Ingestion**

## **Objective**

Pull raw Criteo data from Kaggle and store it exactly as-is in Parquet format (immutable source of truth).
## **Implementation Steps**

### 1.1 Setup Kaggle API

```shell
# Install dependencies
pip install prefect kaggle duckdb pyarrow

# Configure Kaggle credentials
# Place kaggle.json in ~/.kaggle/ or set environment variables
export KAGGLE_USERNAME="your_username"
export KAGGLE_KEY="your_api_key"
```
### 1.2 Prefect Task: Download from Kaggle

```python
@task(name="download_criteo_data", retries=2, retry_delay_seconds=30)
def download_criteo_from_kaggle(dataset_id: str, local_path: str) -> str:
    """
    Downloads Criteo dataset from Kaggle
    dataset_id: 'sharatsachin/criteo-attribution-modeling'
    Returns: path to downloaded CSV
    """
    # Use kaggle API: kaggle datasets download -d {dataset_id}
    # Unzip and return path to criteo_attribution.csv
```

### 1.3 Prefect Task: Create Bronze Parquet

```python
@task(name="load_to_bronze")
def load_to_bronze_layer(csv_path: str) -> str:
    """
    Loads raw CSV into Bronze Parquet using DuckDB
    Returns: path to bronze parquet file
    Schema:
      - impression_id (auto-increment via ROW_NUMBER())
      - timestamp (INTEGER - Unix timestamp from source)
      - uid (TEXT - anonymized user ID)
      - campaign (TEXT)
      - click (INTEGER - 0 or 1)
      - conversion (INTEGER - 0 or 1) 
      - cost (REAL - CPM cost in dollars)
      - conversion_value (REAL - revenue if converted)
      - cat1, cat2, ..., cat9 (TEXT - campaign categories)
    """
    import duckdb
    
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT 
                ROW_NUMBER() OVER () as impression_id,
                *
            FROM read_csv_auto('{csv_path}')
        ) TO 'data/bronze/raw_impressions.parquet' (FORMAT PARQUET)
    """)
    
    return 'data/bronze/raw_impressions.parquet'

```

**Bronze File:** `data/bronze/raw_impressions.parquet`

---
# **Step 2: Silver Layer - Data Cleaning & Validation**

## **Objective**

Clean nulls, fix data types, add basic derived columns, and validate data quality.
## **Implementation Steps**

### 2.1 Prefect Task: Data Quality Checks

```python
@task(name="validate_bronze_data")
def validate_bronze_data(bronze_path: str) -> Dict[str, Any]:
    """
    Runs DQ checks and returns report:
    - Check for null uid (should be 0%)
    - Check for negative cost/conversion_value
    - Check timestamp range (should be within campaign period)
    - Check conversion without click (data quality issue)
    - Count distinct users, campaigns, conversions
    """
    import duckdb
    
    conn = duckdb.connect()
    
    # Run validation queries
    dq_report = {}
    
    # Check null uids
    null_uids = conn.execute(f"""
        SELECT COUNT(*) FROM '{bronze_path}' WHERE uid IS NULL
    """).fetchone()[0]
    dq_report['null_uids'] = null_uids
    
    # Check negative costs
    negative_costs = conn.execute(f"""
        SELECT COUNT(*) FROM '{bronze_path}' WHERE cost < 0
    """).fetchone()[0]
    dq_report['negative_costs'] = negative_costs
    
    # Count distinct entities
    stats = conn.execute(f"""
        SELECT 
            COUNT(DISTINCT uid) as distinct_users,
            COUNT(DISTINCT campaign) as distinct_campaigns,
            SUM(conversion) as total_conversions
        FROM '{bronze_path}'
    """).fetchone()
    
    dq_report.update({
        'distinct_users': stats[0],
        'distinct_campaigns': stats[1],
        'total_conversions': stats[2]
    })
    
    return dq_report
```
### 2.2 Prefect Task: Transform to Silver

```python
@task(name="transform_to_silver")
def transform_to_silver_layer(bronze_path: str) -> str:
    """
    Creates silver.clean_impressions parquet with:
    SQL transformations:
      1. Convert timestamp to DATE and HOUR columns
      2. Remove duplicates (if any) based on (uid, timestamp, campaign)
      3. Filter out impossible records (conversion=1 but cost=0)
      4. Add derived columns:
         - is_weekend (BOOLEAN)
         - event_hour (0-23)
         - time_period (morning/afternoon/evening/night)
    """
    import duckdb
    
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT DISTINCT
                impression_id,
                CAST(to_timestamp(timestamp) AS DATE) as event_date,
                EXTRACT(hour FROM to_timestamp(timestamp)) as event_hour,
                uid,
                campaign,
                CAST(click AS BOOLEAN) as click,
                CAST(conversion AS BOOLEAN) as conversion,
                cost,
                conversion_value,
                EXTRACT(dayofweek FROM to_timestamp(timestamp)) IN (6, 7) as is_weekend,
                CASE 
                    WHEN EXTRACT(hour FROM to_timestamp(timestamp)) BETWEEN 6 AND 11 THEN 'morning'
                    WHEN EXTRACT(hour FROM to_timestamp(timestamp)) BETWEEN 12 AND 17 THEN 'afternoon'
                    WHEN EXTRACT(hour FROM to_timestamp(timestamp)) BETWEEN 18 AND 22 THEN 'evening'
                    ELSE 'night'
                END as time_period,
                cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9
            FROM '{bronze_path}'
            WHERE NOT (conversion = 1 AND cost = 0)  -- Filter data quality issues
        ) TO 'data/silver/clean_impressions.parquet' (FORMAT PARQUET)
    """)
    
    return 'data/silver/clean_impressions.parquet'

```

**Silver File Schema:** `data/silver/clean_impressions.parquet`

| Column             | Type    | Description                                |
| ------------------ | ------- | ------------------------------------------ |
| `impression_id`    | INTEGER | Primary key                                |
| `event_date`       | DATE    | Extracted from timestamp                   |
| `event_hour`       | INTEGER | Hour of day (0-23)                         |
| `uid`              | TEXT    | User ID                                    |
| `campaign`         | TEXT    | Campaign identifier                        |
| `click`            | BOOLEAN | Did user click?                            |
| `conversion`       | BOOLEAN | Did user convert?                          |
| `cost`             | REAL    | Cost in dollars                            |
| `conversion_value` | REAL    | Revenue if converted                       |
| `is_weekend`       | BOOLEAN | Saturday/Sunday flag                       |
| `time_period`      | TEXT    | 'morning', 'afternoon', 'evening', 'night' |

---
# **Step 3: EDA on Silver Layer**

## **Objective**

Answer critical business questions before feature engineering.
## **Key Business Questions for EDA**

### **3.1 Campaign Performance**

> [!question]
> 1. Which campaigns have the highest conversion rates?


```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT campaign, 
           COUNT(*) as impressions,
           SUM(CAST(click AS INTEGER)) as clicks,
           SUM(CAST(conversion AS INTEGER)) as conversions,
           SUM(CAST(conversion AS INTEGER)) * 1.0 / COUNT(*) as cvr,
           SUM(CAST(click AS INTEGER)) * 1.0 / COUNT(*) as ctr
    FROM 'data/silver/clean_impressions.parquet'
    GROUP BY campaign
    ORDER BY conversions DESC
""").df()
```

> [!question]
> 2. What's the cost distribution across campaigns?
> 	- Identify high-spend vs. low-spend campaign
> 	- Check for outliers (campaigns with abnormally high CPM)
> 3. Time-based patterns:
> 	- Do conversions spike on weekends?
> 	- Which hour of the day has best conversion rate?
> 	- Is there a day-of-week effect?


### **3.2 User Behavior**
> [!question]
> 4. What's the distribution of user journey lengths?
> 	- What % of users convert on first impression vs. multi-touch?
> 	- What's the median journey length for converters vs. non-converters?
> 5. Time-to-conversion analysis:
> 	- Among converters, what's the median time from first impression to conversion?
> 	- Are there patterns (e.g., most convert within 3 days)?
> 6. Multi-touch patterns:
> 	- What % of conversions happen after seeing 1, 2, 3, 4+ campaigns?
> 	- Do users who see diverse campaigns convert more?

```python
df = conn.execute("""
    SELECT uid,
           COUNT(*) as touchpoints,
           SUM(CAST(click AS INTEGER)) as total_clicks,
           MAX(CAST(conversion AS INTEGER)) as converted
    FROM 'data/silver/clean_impressions.parquet'
    GROUP BY uid
""").df()
```
### **3.3 Economic Analysis**

> [!question]
> 7. What's the overall ROAS?

```python
df = conn.execute("""
    SELECT SUM(conversion_value) as total_revenue,
           SUM(cost) as total_cost,
           SUM(conversion_value) / SUM(cost) as overall_roas
    FROM 'data/silver/clean_impressions.parquet'
""").df()
```

> [!question]
> 8. Which campaigns are profitable?
> 	- Calculate campaign-level ROAS (you'll use attribution models later, but start with last-touch assumption)
> 9. Cost efficiency:
> 	- What's the Cost Per Click (CPC)?
> 	- What's the Cost Per Acquisition (CPA)?
> 	- Are there diminishing returns (does doubling spend on a campaign double conversions)?

### **3.4 Data Quality Insights**

> [!question]
> 10. Conversion without click anomaly: 
> 	- How many conversions happened without a click? (Should investigate if > 5%)        
> 11. User-level data sufficiency:
> 	- Do we have enough multi-touch users to build attribution models?       
> 	- What % of users have only 1 touchpoint? (These won't help MTA)
> 12. Temporal coverage: 
> 	- How many days does the dataset span?        
> 	- Are there gaps in data? (Missing days could bias analysis)

## **EDA Deliverables**

- **Jupyter notebook** with 10-15 visualizations answering above questions
- **Summary statistics table** saved to `gold.eda_summary`
- **Data quality report** (flag campaigns with < 100 impressions, users with impossible sequences)

---

# **Step 4: Gold Layer - Business-Ready Analytical Tables**

## **Objective**

Create two final tables optimized for attribution modeling and business metric calculation.

---
## **Gold Table 1: `gold.user_journeys`**

**Purpose:** One row per user with their complete journey sequence.
### **Feature Engineering Steps**

```python
@task(name="create_user_journeys")
def create_user_journeys_table(silver_path: str) -> str:
    """
    DuckDB SQL logic:
    1. Group by uid
    2. Use STRING_AGG to create campaign sequence (ordered by timestamp)
    3. Calculate journey-level metrics
    """
    import duckdb
    
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            WITH user_stats AS (
                SELECT 
                    uid,
                    MIN(event_date) as first_touch_date,
                    MAX(event_date) as last_touch_date,
                    COUNT(*) as total_impressions,
                    SUM(CAST(click AS INTEGER)) as total_clicks,
                    SUM(cost) as total_cost,
                    MAX(CAST(conversion AS INTEGER)) as converted,
                    SUM(conversion_value) as conversion_value,
                    STRING_AGG(campaign, '|' ORDER BY impression_id) as campaign_sequence,
                    STRING_AGG(CAST(click AS VARCHAR), '|' ORDER BY impression_id) as click_sequence,
                    COUNT(DISTINCT campaign) as unique_campaigns,
                    FIRST(campaign ORDER BY impression_id) as first_touch_campaign,
                    LAST(campaign ORDER BY impression_id) as last_touch_campaign,
                    SUM(CAST(is_weekend AS INTEGER)) as weekend_touches
                FROM '{silver_path}'
                GROUP BY uid
            ),
            time_gaps AS (
                SELECT 
                    uid,
                    STRING_AGG(
                        CAST(DATEDIFF('day', 
                            LAG(event_date) OVER (PARTITION BY uid ORDER BY impression_id),
                            event_date
                        ) AS VARCHAR), 
                        '|'
                    ) as time_gaps
                FROM '{silver_path}'
                GROUP BY uid
            )
            SELECT 
                u.uid,
                u.first_touch_date,
                u.last_touch_date,
                DATEDIFF('day', u.first_touch_date, u.last_touch_date) as journey_length_days,
                u.total_impressions,
                u.total_clicks,
                u.total_cost,
                u.converted,
                u.conversion_value,
                u.campaign_sequence,
                u.click_sequence,
                t.time_gaps,
                u.unique_campaigns,
                u.first_touch_campaign,
                u.last_touch_campaign,
                CASE WHEN u.unique_campaigns > 1 THEN true ELSE false END as cross_channel_journey,
                u.weekend_touches,
                CASE 
                    WHEN u.total_impressions > 1 
                    THEN u.journey_length_days * 1.0 / (u.total_impressions - 1)
                    ELSE 0 
                END as avg_time_between_touches
            FROM user_stats u
            LEFT JOIN time_gaps t ON u.uid = t.uid
        ) TO 'data/gold/user_journeys.parquet' (FORMAT PARQUET)
    """)
    
    return 'data/gold/user_journeys.parquet'
```
### **Schema**

| Column                     | Type    | Transformation Logic                                      |
| -------------------------- | ------- | --------------------------------------------------------- |
| `uid`                      | TEXT    | Primary key                                               |
| `first_touch_date`         | DATE    | `MIN(event_date)`                                         |
| `last_touch_date`          | DATE    | `MAX(event_date)`                                         |
| `journey_length_days`      | INTEGER | `last_touch_date - first_touch_date`                      |
| `total_impressions`        | INTEGER | `COUNT(*)`                                                |
| `total_clicks`             | INTEGER | `SUM(click)`                                              |
| `total_cost`               | REAL    | `SUM(cost)`                                               |
| `converted`                | BOOLEAN | `MAX(conversion)`                                         |
| `conversion_value`         | REAL    | `SUM(conversion_value)`                                   |
| `campaign_sequence`        | TEXT    | `GROUP_CONCAT(campaign ORDER BY timestamp)` e.g., 'camp_A |
| `click_sequence`           | TEXT    | Sequence of clicks: '0                                    |
| `time_gaps`                | TEXT    | Days between touches: '0                                  |
| `unique_campaigns`         | INTEGER | `COUNT(DISTINCT campaign)`                                |
| `first_touch_campaign`     | TEXT    | Campaign from earliest impression                         |
| `last_touch_campaign`      | TEXT    | Campaign from latest impression                           |
| `cross_channel_journey`    | BOOLEAN | Did user see multiple campaign families?                  |
| `weekend_touches`          | INTEGER | Count of weekend impressions                              |
| `avg_time_between_touches` | REAL    | Average days between impressions                          |

---
## **Gold Table 2: `gold.campaign_touchpoints`**

**Purpose:** One row per impression, but enriched with user-journey context (for attribution modeling).

### Feature Engineering Steps

```python
@task(name="create_campaign_touchpoints")
def create_campaign_touchpoints_table(silver_path: str) -> str:
    """
    Enriches each impression with journey context using window functions
    """
    import duckdb
    
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            WITH journey_context AS (
                SELECT 
                    impression_id,
                    uid,
                    event_date,
                    campaign,
                    click,
                    conversion,
                    cost,
                    conversion_value,
                    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY impression_id) as touchpoint_position,
                    COUNT(*) OVER (PARTITION BY uid) as total_journey_length,
                    MIN(event_date) OVER (PARTITION BY uid) as user_first_touch,
                    MAX(CASE WHEN conversion THEN event_date END) OVER (PARTITION BY uid) as user_conversion_date,
                    LAG(campaign) OVER (PARTITION BY uid ORDER BY impression_id) as previous_campaign,
                    SUM(CASE WHEN campaign = c.campaign THEN 1 ELSE 0 END) 
                        OVER (PARTITION BY uid, campaign ORDER BY impression_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
                        as campaign_exposure_count
                FROM '{silver_path}' c
            )
            SELECT 
                impression_id,
                uid,
                event_date,
                campaign,
                click,
                conversion,
                cost,
                conversion_value,
                touchpoint_position,
                CASE WHEN touchpoint_position = 1 THEN true ELSE false END as is_first_touch,
                CASE WHEN touchpoint_position = total_journey_length AND conversion THEN true ELSE false END as is_last_touch,
                total_journey_length,
                DATEDIFF('day', user_first_touch, event_date) as days_since_first_touch,
                CASE 
                    WHEN user_conversion_date IS NOT NULL 
                    THEN DATEDIFF('day', event_date, user_conversion_date)
                    ELSE NULL 
                END as days_until_conversion,
                touchpoint_position * 1.0 / total_journey_length as position_normalized,
                CASE WHEN campaign_exposure_count > 1 THEN true ELSE false END as is_repeat_campaign,
                campaign_exposure_count,
                CASE WHEN previous_campaign IS NOT NULL AND previous_campaign != campaign THEN true ELSE false END as cross_channel_flag
            FROM journey_context
        ) TO 'data/gold/campaign_touchpoints.parquet' (FORMAT PARQUET)
    """)
    
    return 'data/gold/campaign_touchpoints.parquet'
```
### **Schema**

| Column                       | Type    | Transformation Logic                                     |
| ---------------------------- | ------- | -------------------------------------------------------- |
| `impression_id`              | INTEGER | Primary key                                              |
| `uid`                        | TEXT    | Foreign key to user_journeys                             |
| `timestamp`                  | INTEGER | Original timestamp                                       |
| `event_date`                 | DATE    | From silver                                              |
| `campaign`                   | TEXT    | Campaign ID                                              |
| `click`                      | BOOLEAN | From silver                                              |
| `conversion`                 | BOOLEAN | From silver                                              |
| `cost`                       | REAL    | From silver                                              |
| `conversion_value`           | REAL    | From silver                                              |
| **Journey Context Features** |         |                                                          |
| `touchpoint_position`        | INTEGER | ROW_NUMBER() within user journey (1, 2, 3...)            |
| `is_first_touch`             | BOOLEAN | `touchpoint_position = 1`                                |
| `is_last_touch`              | BOOLEAN | Is this the final impression before conversion?          |
| `total_journey_length`       | INTEGER | Total impressions for this user                          |
| `days_since_first_touch`     | INTEGER | Time from user's first impression                        |
| `days_until_conversion`      | INTEGER | Time until conversion (NULL if no conversion)            |
| `position_normalized`        | REAL    | `touchpoint_position / total_journey_length` (0-1 scale) |
| **Path Analysis Features**   |         |                                                          |
| `is_repeat_campaign`         | BOOLEAN | Did user see this campaign before?                       |
| `campaign_exposure_count`    | INTEGER | How many times user saw THIS campaign so far             |
| `cross_channel_flag`         | BOOLEAN | Is this a different campaign family than previous touch? |

---
# **Step 5: Prefect Flow Orchestration**

```python
from prefect import flow, task
from typing import Dict, Any

@flow(name="criteo_etl_pipeline")
def criteo_attribution_etl():
    """
    End-to-end pipeline from Kaggle to Gold parquet files
    """
    # Bronze Layer
    csv_path = download_criteo_from_kaggle(
        dataset_id="sharatsachin/criteo-attribution-modeling",
        local_path="./data/raw/"
    )
    
    bronze_path = load_to_bronze_layer(csv_path=csv_path)
    
    # Silver Layer
    dq_report = validate_bronze_data(bronze_path=bronze_path)
    
    silver_path = transform_to_silver_layer(bronze_path=bronze_path)
    
    # EDA (optional - can be separate notebook)
    # eda_summary = run_eda_queries(silver_path=silver_path)
    
    # Gold Layer
    user_journeys_path = create_user_journeys_table(silver_path=silver_path)
    touchpoints_path = create_campaign_touchpoints_table(silver_path=silver_path)
    
    return {
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "user_journeys_path": user_journeys_path,
        "touchpoints_path": touchpoints_path,
        "data_quality": dq_report
    }
```
---

# **Key Engineering Decisions**

## **Why DuckDB for this project?**

- **Pros:** Columnar storage optimized for analytical queries (GROUP BY, window functions), 15-50x faster than row-based databases for aggregations, native Parquet support eliminates ETL overhead
- **Cons:** None for this use case (batch analytical workload)
## **Why Parquet files?**

- **Industry standard** for data lakes (used by Snowflake, BigQuery, Databricks)    
- **Compression:** Smaller file sizes than CSV or database files
- **DuckDB integration:** Query Parquet directly without loading into memory
- **Version control friendly:** Can use Git LFS or cloud storage
## **Why medallion architecture?**

- **Bronze = Immutable audit trail:** If you discover a bug in transformation logic, you can reprocess from bronze without re-downloading
- **Silver = Clean foundation:** Separates "data engineering" from "feature engineering"
- **Gold = Business-specific:** Optimized for your specific analysis (attribution models, RFM)
## **Handling large data**

The Criteo dataset is ~2.4 GB. DuckDB handles this efficiently:
- Automatic parallelization across CPU cores
- Vectorized execution for fast aggregations
- Direct Parquet scanning without full data load
- For memory constraints, DuckDB automatically spills to disk
---
## **Phase 1 Deliverables Checklist**

- [ ]  `prefect_flows/criteo_etl.py` - Main pipeline script
- [ ]  `data/bronze/raw_impressions.parquet` - Raw data
- [ ]  `data/silver/clean_impressions.parquet` - Cleaned data
- [ ]  `data/gold/user_journeys.parquet` - User-level features
- [ ]  `data/gold/campaign_touchpoints.parquet` - Impression-level features
- [ ]  `notebooks/01_eda.ipynb` - EDA answering 12 business questions
- [ ]  `data/gold/eda_summary.parquet` - Summary statistics
- [ ]  `outputs/data_quality_report.json` - Validation results
- [ ]  `README.md` - Documentation of schema decisions
