## Phase 1: Data Acquisition and Preparation

The Phase 1 pipeline downloads the Criteo Attribution Modeling dataset, loads it to a bronze layer, validates data quality, transforms to a silver layer, and builds gold **user_journeys** tables. CPO modeling and campaign_touchpoints will be introduced in Phase 2.

**How to run:** From the project root, execute:

```bash
python prefect_flows/criteo_etl.py
```

**Prerequisites:** Kaggle credentials configured for [kagglehub](https://github.com/Kaggle/kagglehub) (e.g., `~/.kaggle/kaggle.json` or `KAGGLE_USERNAME`/`KAGGLE_KEY` environment variables).

For full details on data fields, transformations, and checks, see [Phase 1: Data Acquisition and Preparation](docs/phase-1-data-acquisition-and-preparation.md). Detailed timings are logged by Prefect for profiling.

---

## To do
- [ ] Optimize the User Journeys query
- [ ] EDA on Silver layer to answer some Business Questions
- [ ] 