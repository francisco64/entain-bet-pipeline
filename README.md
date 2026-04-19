# Entain Bet Pipeline

## Setup

### Package

- small Python package under `src/`
- package metadata in `pyproject.toml`
- CLI entry point: `bet-pipeline`
- Docker support with `Dockerfile`
- `pytest` only for testing, so it stays in optional development dependencies

### Folder structure

```text
pyproject.toml
Dockerfile
pytest.ini
src/
  bet_pipeline/
    __init__.py
    cli.py
    schema.py
    validate.py
    build_features.py
tests/
  test_validation_pipeline.py
  test_feature_build.py
```

### CLI

```bash
bet-pipeline validate --input bets.csv --output outputs/validation
bet-pipeline build-features --input bets.csv --output outputs/features
```

- `validate` reads the raw CSV and writes the validation outputs.
- `build-features` accepts the raw CSV path, runs validation internally, and builds features from validated bets.

### Docker

Build:

```bash
docker build -t entain-bet-pipeline .
```

Run validation:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline \
  validate --input /data/bets.csv --output /outputs/validation/
```

Run feature generation:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline \
  build-features --input /data/bets.csv --output /outputs/features/
```

### Outputs

| Stage | Output |
| --- | --- |
| Validation | `valid_bets.parquet` |
| Validation | `invalid_bets.csv` |
| Validation | `validation_report.json` |
| Features | `customer_features.parquet` |

## Assumptions And Design Choices

- `valid_bets` is written as parquet because parquet is a columnar format more efficient for downstream analytical processing.
- `invalid_bets` is written as CSV because failed rows are mainly for human inspection, debugging, and reporting.
- `invalid_reasons` is is a column proposed inside `invalid_bets` as a comma-separated field for observability, because one row can fail multiple rules.
- `expected_payout` and `expected_return_for_entain` are included in `invalid_bets.csv` to allow direct comparison against the actual values.
- `validation_report.json` includes first-20-window health checks because the quality of that window affects feature generation and downstream model quality.
- The customer window is defined as the first 20 raw bets of the user, meaning raw `bet_num <= 20`.
- This raw first-20 definition preserves early behavior instead of replacing invalid early bets with later bets.
- `bets_used` is defined as the number of valid bets actually used inside the raw first-20 window.
- `first_bet_datetime` is the first valid bet used in that window.
- `twentieth_bet_datetime` is the valid row where raw `bet_num = 20`; if that row is missing or invalid, the value is `null`.
- Using only valid rows inside the raw first-20 window is justified by the observed data quality. In `validation_report.json`, `pct_invalid_bets_in_first_20_window` is `0.00212`, meaning about `99.8%` of bets in that window are valid. 
- The report also shows only `9` users with an invalid first bet out of `5000` so the decision of making `first_bet_datetime` the first valid bet will not influence much the downstream analysis.
- Feature generation is implemented in DuckDB.
- DuckDB was chosen for three reasons:
  - parquet + DuckDB is efficient for this style of feature table
  - the logic is mostly SQL-style aggregation and windowing
  - DuckDB can work directly over parquet files but integration with datalakes with parquet files in AWS S3 or Postgres for production is seemless

## Validation Checks

| Done | Check | Why it matters |
| --- | --- | --- |
| `✓` | `bet_id` is unique | Each row should represent one real bet. |
| `✓` | `bet_datetime` is a valid datetime | Feature timestamps are not trustworthy otherwise. |
| `✓` | `bet_num` is a positive integer | The first-20 window depends on valid ordering. |
| `✓` | `customer_id + bet_num` is unique | A customer should not have two different bets with the same sequence number. |
| `✓` | `betting_amount > 0` | Zero or negative stake breaks the business meaning of a bet. |
| `✓` | `price > 1` | Decimal odds must be greater than 1 by definition. |
| `✓` | `category` is `sports` or `racing` | Keeps the domain values consistent for features. |
| `✓` | `stake_type` is `cash` or `bonus` | The payout rules depend on this field. |
| `✓` | `bet_result` is `return` or `no-return` | The payout and return formulas depend on this field. |
| `✓` | `payout` matches the task formula | Prevents financially inconsistent rows from entering features. |
| `✓` | `return_for_entain` matches the task formula | Prevents wrong profit/loss values from entering features. |

## Validation Report

### Main fields

| Field |
| --- |
| `total_bets_input` |
| `total_valid_bets` |
| `total_invalid_bets` |
| `unique_customers_input` |
| `unique_customers_with_valid_bets` |
| `users_removed_all_bets_invalid` |
| `invalid_by_rule` |

### First-20 window

| Done | Check | Why it matters |
| --- | --- | --- |
| `✓` | `window_definition` is raw `bet_num` between 1 and 20 inclusive | Makes the feature window explicit and easy to explain. |
| `✓` | `users_with_at_least_1_invalid_bet_in_first_20` | Shows how many customers have damaged early-history data. |
| `✓` | `total_bets_in_first_20_window` | Shows the size of the raw window used for feature quality checks. |
| `✓` | `invalid_bets_in_first_20_window` | Shows how much bad data exists inside the most important modeling window. |
| `✓` | `pct_invalid_bets_in_first_20_window` | Gives an easy quality ratio instead of only raw counts. |
| `✓` | `users_with_invalid_first_bet` | Highlights customers whose very first recorded behavior is unusable. |

## Feature Table

### Output columns

| Column |
| --- |
| `customer_id` |
| `first_bet_datetime` |
| `twentieth_bet_datetime` |
| `bets_used` |
| `total_betting_amount` |
| `mean_betting_amount` |
| `mean_price` |
| `pct_racing` |
| `pct_cash` |
| `pct_return` |
| `total_payout` |
| `total_return_for_entain` |
| `feature_generated_at` |

### Definitions

| Field | Definition |
| --- | --- |
| `bets_used` | Number of valid bets actually used inside the raw first-20 window. |
| `first_bet_datetime` | Datetime of the first valid bet used. |
| `twentieth_bet_datetime` | Datetime of the valid row where raw `bet_num = 20`; otherwise `null`. |

## Tests

| Test file | What it checks |
| --- | --- |
| `tests/test_validation_pipeline.py` | Known invalid rows produce the expected validation report and invalid output fields. |
| `tests/test_feature_build.py` | Features use only valid bets inside raw `bet_num <= 20`. |

```bash
python -m pytest -q
```
