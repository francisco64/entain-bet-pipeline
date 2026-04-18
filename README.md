# Entain Bet Pipeline

This project builds a small local batch pipeline around `bets.csv`.

At this stage I only implemented the Python core logic under `src/bet_pipeline`. I have not added CLI wiring, tests, Docker, or packaging details yet because I wanted to get the data logic clear first.

## What the pipeline does

There are two stages.

### 1. Validation

The input CSV is treated as untrusted data.

The validation stage reads `bets.csv` and writes:

- `valid_bets.parquet`
- `invalid_bets.csv`
- `validation_report.json`

Invalid rows are not dropped silently. They stay visible in `invalid_bets.csv` with an `invalid_reasons` column. That column is a comma-separated list because one row can fail more than one rule.

### 2. Customer features

The feature stage reads `valid_bets.parquet` and writes:

- `customer_features.parquet`

This output has one row per customer.

## Main design choices

### Raw `bet_num` is the ordering field

I treat `bet_num` as the source of truth for bet order because the brief says to do that.

### "First 20 bets" means raw bets where `bet_num <= 20`

I did not define this as "first 20 valid bets".

Instead, I keep the early customer window fixed and only use valid bets that fall inside that raw window. If a bet inside the first 20 is invalid, I exclude it from the feature calculations, but I do not replace it with bet 21 or later.

That makes the behavior easier to explain and keeps the meaning of "first 20 bets" intact.

### Validation and testing are different things

Validation is the pipeline logic that checks the real dataset against the business rules.

Tests are separate software checks that I would add later to verify that the validation and feature logic behave correctly on small controlled examples.

## Validation rules

Required columns:

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

Business rules:

- `betting_amount > 0`
- `price > 1`
- `category` must be `sports` or `racing`
- `stake_type` must be `cash` or `bonus`
- `bet_result` must be `return` or `no-return`

Payout rules:

- `no-return` -> `payout = 0`
- `return + cash` -> `payout = betting_amount * price`
- `return + bonus` -> `payout = betting_amount * (price - 1)`

Return for Entain rules:

- `no-return + cash` -> `return_for_entain = betting_amount`
- `no-return + bonus` -> `return_for_entain = 0`
- `return + cash` -> `return_for_entain = betting_amount - payout`
- `return + bonus` -> `return_for_entain = -payout`

Pragmatic consistency checks:

- `bet_num` must be a positive integer
- duplicate `bet_id` is invalid
- duplicate `customer_id + bet_num` is invalid
- invalid `bet_datetime` is invalid

## Validation report

The report includes:

- total input bets
- total valid bets
- total invalid bets
- unique customers in the raw file
- unique customers with at least one valid bet
- users removed because all of their bets were invalid
- invalid counts by rule

It also includes a `first_20_window_health` section because the first 20 bets are the whole basis of the feature table.

That section includes:

- how the window is defined
- how many users have at least one invalid bet in that window
- how many raw bets fall in that window
- how many of those are invalid
- the percentage invalid in that window
- how many users have an invalid first bet

## Feature table

The feature output contains one row per customer and includes at least:

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

- `bets_used` = number of valid bets actually used in the raw first-20 window
- `first_bet_datetime` = datetime of the first valid bet used
- `twentieth_bet_datetime` = datetime of the valid row where `bet_num = 20`; otherwise null

## Why pandas for validation and DuckDB for features

I used pandas for validation because the job is mostly row-level checks and explicit invalid-row handling.

I used DuckDB for feature generation because the second stage is just a clean tabular aggregation problem over validated data, and DuckDB works naturally with parquet.

## Current project files

```text
src/
  bet_pipeline/
    __init__.py
    schema.py
    validate.py
    build_features.py
```

## How to run the current core pipeline

There is no CLI yet, so the current flow is to call the Python functions directly:

```python
from bet_pipeline.validate import run_validation
from bet_pipeline.build_features import run_feature_build

run_validation("bets.csv", "outputs/validation")
run_feature_build("outputs/validation/valid_bets.parquet", "outputs/features")
```

## Notes on style

I intentionally kept the code simple and explicit.

For this exercise I think readability matters more than squeezing everything into abstractions. The goal is that someone reviewing the project can understand the data flow quickly and that I can explain the design choices clearly in an interview.
