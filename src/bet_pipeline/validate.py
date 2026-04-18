import json
from pathlib import Path

import numpy as np
import pandas as pd

from bet_pipeline.schema import (
    ALLOWED_BET_RESULTS,
    ALLOWED_CATEGORIES,
    ALLOWED_STAKE_TYPES,
    FIRST_20_MAX_BET_NUM,
    FLOAT_TOLERANCE,
    NUMERIC_COLUMNS,
    REQUIRED_COLUMNS,
)


def load_bets_csv(input_path):
    df = pd.read_csv(input_path)

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    df = df[REQUIRED_COLUMNS].copy()

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["bet_datetime"] = pd.to_datetime(df["bet_datetime"], errors="coerce")

    return df


def calculate_expected_payout(df):
    expected_payout = pd.Series(np.nan, index=df.index)

    no_return = df["bet_result"] == "no-return"
    return_cash = (df["bet_result"] == "return") & (df["stake_type"] == "cash")
    return_bonus = (df["bet_result"] == "return") & (df["stake_type"] == "bonus")

    expected_payout.loc[no_return] = 0.0
    expected_payout.loc[return_cash] = (
        df.loc[return_cash, "betting_amount"] * df.loc[return_cash, "price"]
    )
    expected_payout.loc[return_bonus] = (
        df.loc[return_bonus, "betting_amount"] * (df.loc[return_bonus, "price"] - 1.0)
    )

    return expected_payout


def calculate_expected_return_for_entain(df, expected_payout):
    expected_return_for_entain = pd.Series(np.nan, index=df.index)

    no_return_cash = (df["bet_result"] == "no-return") & (df["stake_type"] == "cash")
    no_return_bonus = (df["bet_result"] == "no-return") & (df["stake_type"] == "bonus")
    return_cash = (df["bet_result"] == "return") & (df["stake_type"] == "cash")
    return_bonus = (df["bet_result"] == "return") & (df["stake_type"] == "bonus")

    expected_return_for_entain.loc[no_return_cash] = df.loc[no_return_cash, "betting_amount"]
    expected_return_for_entain.loc[no_return_bonus] = 0.0
    expected_return_for_entain.loc[return_cash] = (
        df.loc[return_cash, "betting_amount"] - expected_payout.loc[return_cash]
    )
    expected_return_for_entain.loc[return_bonus] = -expected_payout.loc[return_bonus]

    return expected_return_for_entain


def validate_bets(df):
    # Reset the index here because validate_bets can be called directly.
    df = df.copy().reset_index(drop=True)

    # bet_num > 0 and integer
    bet_num_is_positive_integer = (
        df["bet_num"].notna()
        & (df["bet_num"] > 0)
        & np.isclose(df["bet_num"] % 1, 0.0, atol=FLOAT_TOLERANCE)
    )

    # Expected payout and return_for_entain from the task rules
    expected_payout = calculate_expected_payout(df)
    expected_return_for_entain = calculate_expected_return_for_entain(df, expected_payout)
    df["expected_payout"] = expected_payout
    df["expected_return_for_entain"] = expected_return_for_entain

    # Fields needed before checking payout formulas
    calculation_inputs_are_valid = (
        df["betting_amount"].notna()
        & df["price"].notna()
        & df["stake_type"].isin(ALLOWED_STAKE_TYPES)
        & df["bet_result"].isin(ALLOWED_BET_RESULTS)
    )

    validation_rules = {
        # bet_id must be unique
        "duplicate_bet_id": df["bet_id"].duplicated(keep=False),
        # bet_datetime must be a valid datetime
        "invalid_bet_datetime": df["bet_datetime"].isna(),
        # bet_num must be a positive integer
        "bet_num_not_positive_integer": ~bet_num_is_positive_integer,
        # customer_id + bet_num must be unique
        "duplicate_customer_bet_num": (
            df["customer_id"].notna()
            & bet_num_is_positive_integer
            & df.duplicated(subset=["customer_id", "bet_num"], keep=False)
        ),
        # betting_amount > 0
        "betting_amount_not_gt_0": ~(df["betting_amount"] > 0),
        # price > 1
        "price_not_gt_1": ~(df["price"] > 1),
        # category in {sports, racing}
        "invalid_category": ~df["category"].isin(ALLOWED_CATEGORIES),
        # stake_type in {cash, bonus}
        "invalid_stake_type": ~df["stake_type"].isin(ALLOWED_STAKE_TYPES),
        # bet_result in {return, no-return}
        "invalid_bet_result": ~df["bet_result"].isin(ALLOWED_BET_RESULTS),
        # payout must match the business rule formula
        "payout_mismatch": calculation_inputs_are_valid
        & ~np.isclose(
            df["payout"],
            expected_payout,
            atol=FLOAT_TOLERANCE,
            rtol=0.0,
            equal_nan=False,
        ),
        # return_for_entain must match the business rule formula
        "return_for_entain_mismatch": calculation_inputs_are_valid
        & ~np.isclose(
            df["return_for_entain"],
            expected_return_for_entain,
            atol=FLOAT_TOLERANCE,
            rtol=0.0,
            equal_nan=False,
        ),
    }

    # Collect every failed rule for each row
    invalid_reasons = [[] for _ in range(len(df))]

    for rule_name, failed_rows in validation_rules.items():
        for index in df.index[failed_rows]:
            invalid_reasons[index].append(rule_name)

    df["invalid_reasons"] = [",".join(reasons) for reasons in invalid_reasons]
    df["is_valid"] = df["invalid_reasons"] == ""

    valid_bets = df.loc[df["is_valid"], REQUIRED_COLUMNS].copy()
    invalid_bets = df.loc[
        ~df["is_valid"],
        REQUIRED_COLUMNS
        + [
            "expected_payout",
            "expected_return_for_entain",
            "invalid_reasons",
        ],
    ].copy()

    total_bets_input = int(len(df))
    total_valid_bets = int(df["is_valid"].sum())
    total_invalid_bets = total_bets_input - total_valid_bets

    # Health metrics for the raw first-20-bet window
    first_20_bets = df.loc[
        bet_num_is_positive_integer & (df["bet_num"] <= FIRST_20_MAX_BET_NUM),
        ["customer_id", "bet_num", "is_valid"],
    ].copy()

    total_bets_in_first_20_window = int(len(first_20_bets))
    invalid_bets_in_first_20_window = int((~first_20_bets["is_valid"]).sum())

    if total_bets_in_first_20_window == 0:
        pct_invalid_bets_in_first_20_window = 0.0
        users_with_at_least_1_invalid_bet_in_first_20 = 0
        users_with_invalid_first_bet = 0
    else:
        users_with_at_least_1_invalid_bet_in_first_20 = int(
            first_20_bets.loc[~first_20_bets["is_valid"], "customer_id"].nunique()
        )
        users_with_invalid_first_bet = int(
            first_20_bets.loc[
                (first_20_bets["bet_num"] == 1) & (~first_20_bets["is_valid"]),
                "customer_id",
            ].nunique()
        )
        pct_invalid_bets_in_first_20_window = (
            invalid_bets_in_first_20_window / total_bets_in_first_20_window
        )

    valid_bets_per_customer = df.groupby("customer_id")["is_valid"].sum()

    validation_report = {
        "total_bets_input": total_bets_input,
        "total_valid_bets": total_valid_bets,
        "total_invalid_bets": total_invalid_bets,
        "unique_customers_input": int(df["customer_id"].nunique()),
        "unique_customers_with_valid_bets": int(df.loc[df["is_valid"], "customer_id"].nunique()),
        "users_removed_all_bets_invalid": int((valid_bets_per_customer == 0).sum()),
        "invalid_by_rule": {
            rule_name: int(failed_rows.sum())
            for rule_name, failed_rows in validation_rules.items()
        },
        "first_20_window_health": {
            "window_definition": f"raw bet_num between 1 and {FIRST_20_MAX_BET_NUM} inclusive",
            "users_with_at_least_1_invalid_bet_in_first_20": (
                users_with_at_least_1_invalid_bet_in_first_20
            ),
            "total_bets_in_first_20_window": total_bets_in_first_20_window,
            "invalid_bets_in_first_20_window": invalid_bets_in_first_20_window,
            "pct_invalid_bets_in_first_20_window": pct_invalid_bets_in_first_20_window,
            "users_with_invalid_first_bet": users_with_invalid_first_bet,
        },
    }

    return valid_bets, invalid_bets, validation_report


def run_validation(input_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    bets = load_bets_csv(input_path)
    valid_bets, invalid_bets, validation_report = validate_bets(bets)

    valid_bets.to_parquet(output_dir / "valid_bets.parquet", index=False)
    invalid_bets.to_csv(output_dir / "invalid_bets.csv", index=False)

    with (output_dir / "validation_report.json").open("w", encoding="utf-8") as file:
        json.dump(validation_report, file, indent=2)

    return validation_report
