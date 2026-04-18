# Entain Bet Pipeline

## Overview

This project implements a small local batch pipeline around `bets.csv`.

The task has two core stages:

1. validate the raw betting data against the business rules
2. build a customer-level feature table from each customer’s first 20 bets

I treated this as an engineering exercise more than a pure coding exercise. The code is intentionally simple and explicit, and this README explains the main assumptions, logic, and design choices so the behavior is easy to review and defend.

## What the pipeline produces

### Validation stage

Input:

- raw `bets.csv`

Outputs:

- `valid_bets.parquet`
- `invalid_bets.csv`
- `validation_report.json`

### Feature stage

Input:

- validated bets

Output:

- `customer_features.parquet`

## Local system view

The local flow is:

1. read the raw CSV as untrusted input
2. apply schema, domain, numeric, and formula validation
3. split the data into:
   - curated valid bets
   - explicit invalid bets
   - a machine-readable validation report
4. build customer features only from validated bets
5. write parquet outputs for downstream analytical use

This is intentionally batch-oriented, local, and reproducible.

## Main assumptions

These assumptions drive the implementation:

1. `bet_num` is the authoritative ordering field
   The challenge explicitly says to use `bet_num` as the main ordering field, so I do not rely on `bet_datetime` for sequencing.

2. The raw CSV is untrusted input
   I do not assume rows are correct or that only one field can be wrong.

3. Invalid records should remain inspectable
   Invalid rows are written out explicitly instead of being silently dropped.

4. The feature table should represent early customer behavior
   The customer window is defined from the raw first 20 bets, not from the first 20 valid bets.

5. `pytest` is only needed for testing
   That is why it is placed in optional development dependencies in `pyproject.toml` rather than the runtime dependency list.

## Most important design choices

### 1. Invalid rows are kept explicit

`invalid_bets.csv` includes:

- the original row values
- `expected_payout`
- `expected_return_for_entain`
- `invalid_reasons`

This is deliberate.

The comma-separated `invalid_reasons` field allows one row to show every failed rule in one place, which is useful for debugging and reporting.

The expected formula columns make payout and return mismatches easy to inspect without recalculating them by hand.

### 2. The first-20 window uses raw `bet_num <= 20`

This is the central modeling choice.

I did not define the feature window as “the first 20 valid bets”. Instead:

- the customer window is all raw bets where `bet_num` is between 1 and 20 inclusive
- only valid bets inside that raw window are used in feature calculations
- invalid early bets are not replaced with later bets beyond 20

Why:

- it keeps the meaning of “first 20 bets” intact
- it preserves early customer behavior instead of silently shifting the window
- it makes the logic deterministic and easy to explain

### 3. Validation and tests are different concerns

Validation is part of the runtime pipeline.

Tests are the safety layer around the pipeline code.

That distinction matters in this project because:

- validation acts on the real input dataset
- tests check that the pipeline logic behaves correctly on known examples

### 4. Pandas for validation, DuckDB for feature generation

I used:

- `pandas` for validation because row-level checks and explicit invalid-record handling are easy to express there
- `DuckDB` for features because the second stage is mostly filtering and aggregation over parquet-style tabular data

This felt like the simplest tool choice for the actual work being done.

## Validation rules

### Required columns

- `bet_id`
- `customer_id`
- `bet_datetime`
- `bet_num`
- `betting_amount`
- `price`
- `category`
- `stake_type`
- `bet_result`
- `payout`
- `return_for_entain`

### Business rules

- `betting_amount > 0`
- `price > 1`
- `category` must be `sports` or `racing`
- `stake_type` must be `cash` or `bonus`
- `bet_result` must be `return` or `no-return`

### Payout rules

- `no-return` -> `payout = 0`
- `return + cash` -> `payout = betting_amount * price`
- `return + bonus` -> `payout = betting_amount * (price - 1)`

### Return for Entain rules

- `no-return + cash` -> `return_for_entain = betting_amount`
- `no-return + bonus` -> `return_for_entain = 0`
- `return + cash` -> `return_for_entain = betting_amount - payout`
- `return + bonus` -> `return_for_entain = -payout`

### Pragmatic consistency checks

- `bet_num` must be a positive integer
- duplicate `bet_id` is invalid
- duplicate `customer_id + bet_num` is invalid
- invalid `bet_datetime` is invalid

### Validation checks summary

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

## Validation report

`validation_report.json` includes:

- `total_bets_input`
- `total_valid_bets`
- `total_invalid_bets`
- `unique_customers_input`
- `unique_customers_with_valid_bets`
- `users_removed_all_bets_invalid`
- `invalid_by_rule`

It also includes `first_20_window_health`, because the first-20-bet window is the basis of the feature table.

### First-20 window health checks

| Done | Check | Why it matters |
| --- | --- | --- |
| `✓` | `window_definition` is raw `bet_num` between 1 and 20 inclusive | Makes the feature window explicit and easy to explain. |
| `✓` | `users_with_at_least_1_invalid_bet_in_first_20` | Shows how many customers have damaged early-history data. |
| `✓` | `total_bets_in_first_20_window` | Shows the size of the raw window used for feature quality checks. |
| `✓` | `invalid_bets_in_first_20_window` | Shows how much bad data exists inside the most important modeling window. |
| `✓` | `pct_invalid_bets_in_first_20_window` | Gives an easy quality ratio instead of only raw counts. |
| `✓` | `users_with_invalid_first_bet` | Highlights customers whose very first recorded behavior is unusable. |

## Feature table

The feature output contains one row per customer and includes:

- `customer_id`
- `first_bet_datetime`
- `twentieth_bet_datetime`
- `bets_used`
- `total_betting_amount`
- `mean_betting_amount`
- `mean_price`
- `pct_racing`
- `pct_cash`
- `pct_return`
- `total_payout`
- `total_return_for_entain`
- `feature_generated_at`

Definitions:

- `bets_used` = number of valid bets actually used inside the raw first-20 window
- `first_bet_datetime` = datetime of the first valid bet used
- `twentieth_bet_datetime` = datetime of the valid row where raw `bet_num = 20`; otherwise null

## Package structure

The solution is packaged under `src/`:

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

## CLI

The package exposes a CLI entry point called `bet-pipeline`.

Supported commands:

```bash
bet-pipeline validate --input bets.csv --output outputs/validation
bet-pipeline build-features --input bets.csv --output outputs/features
```

The `build-features` command accepts the raw CSV path, runs validation internally in a temporary folder, and then builds the feature parquet from validated data. I chose that interface because it matches the challenge wording while still keeping the feature stage dependent on validated bets.

You can also run the Python functions directly:

```python
from bet_pipeline.validate import run_validation
from bet_pipeline.build_features import run_feature_build

run_validation("bets.csv", "outputs/validation")
run_feature_build("outputs/validation/valid_bets.parquet", "outputs/features")
```

## Installation

Package metadata is defined in `pyproject.toml`.

Runtime dependencies:

- `duckdb`
- `numpy`
- `pandas`
- `pyarrow`

Testing dependency:

- `pytest` under optional development dependencies

## Docker

The project includes a simple Dockerfile so the pipeline can run in a clean environment.

Build the image:

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

Note on paths:

- the `/data/...` and `/outputs/...` paths are container paths
- local non-Docker runs can use either relative paths like `./bets.csv` or real absolute paths on your machine

## Testing approach

I wanted the tests to stay small and readable.

So instead of creating a large test framework, I added two focused pytest files:

### `test_validation_pipeline.py`

Uses a small hard-coded dataset with known invalid cases.

Why this test exists:

- to check that validation counts are correct on known bad input
- to check that `validation_report.json` matches the expected report
- to check that `invalid_bets.csv` keeps debugging information such as `expected_payout`, `expected_return_for_entain`, and comma-separated `invalid_reasons`

### `test_feature_build.py`

Uses a tiny valid parquet input.

Why this test exists:

- to protect the most important feature-generation rule
- to verify that only bets inside raw `bet_num <= 20` are used
- to verify that later bets are not pulled in to replace missing early bets
- to verify that `twentieth_bet_datetime` comes from the valid row with raw `bet_num = 20`

Run tests with:

```bash
python -m pytest -q
```

## Engineering trade-offs

This solution intentionally favors readability over abstraction.

Examples:

- validation logic is written directly instead of being hidden behind a framework
- the test setup is hard-coded and small rather than highly reusable
- the CLI is minimal and only supports the required commands
- the outputs are chosen for practical inspection and downstream use, not for architectural sophistication

If this were extended further, the next likely additions would be:

- more tests around edge cases
- logging and metrics around pipeline runs
- stricter environment pinning for packaging
- the architecture diagram and broader system design note requested in the later task sections

## Final note

The main objective of this submission is to make the pipeline logic easy to trust.

The implementation is deliberately modest: small functions, explicit rules, inspectable outputs, and tests tied to the main business decisions. That felt more appropriate for this exercise than adding more abstraction or framework code than the task really needs.
