import json

import pandas as pd

from bet_pipeline.validate import run_validation


def build_invalid_bets_dataframe():
    rows = [
        {
            "bet_id": 104760,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-14 16:25:02.036",
            "bet_num": 1,
            "betting_amount": 19.5,
            "price": 7.56,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "return",
            "payout": 147.42,
            "return_for_entain": -127.92,
        },
        {
            "bet_id": 100278,
            "customer_id": "fdea8674-fd2b-486a-8709-04365c4cdaf2",
            "bet_datetime": "2024-09-02 03:42:12.034",
            "bet_num": 1,
            "betting_amount": 0.0,
            "price": 12.55,
            "category": "racing",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 1.0,
            "return_for_entain": 2.0,
        },
        {
            "bet_id": 59981,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-04 15:57:04.004",
            "bet_num": 2,
            "betting_amount": 10.1,
            "price": 1.0,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 10.1,
        },
        {
            "bet_id": 37835,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-12 14:04:21.017",
            "bet_num": 3,
            "betting_amount": 9.1,
            "price": 13.7,
            "category": "casino",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 9.1,
        },
        {
            "bet_id": 367491,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-25 18:14:04.013",
            "bet_num": 4,
            "betting_amount": 2.5,
            "price": 3.58,
            "category": "sports",
            "stake_type": "voucher",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 2.5,
        },
        {
            "bet_id": 108959,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-18 04:08:15.043",
            "bet_num": 5,
            "betting_amount": 2.9,
            "price": 13.93,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "pending",
            "payout": 0.0,
            "return_for_entain": 2.9,
        },
        {
            "bet_id": 286183,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-10-22 22:15:22.043",
            "bet_num": 6,
            "betting_amount": 2.7,
            "price": 8.09,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "return",
            "payout": 22.0,
            "return_for_entain": -19.143,
        },
        {
            "bet_id": 47748,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-11-03 02:19:32.022",
            "bet_num": 7,
            "betting_amount": 3.9,
            "price": 14.61,
            "category": "sports",
            "stake_type": "bonus",
            "bet_result": "return",
            "payout": 53.079,
            "return_for_entain": -52.0,
        },
        {
            "bet_id": 216451,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-11-08 20:55:30.016",
            "bet_num": 8,
            "betting_amount": 9.4,
            "price": 13.56,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 9.4,
        },
        {
            "bet_id": 216451,
            "customer_id": "0f6e1cdd-a827-44a1-9357-cc6206c3ddbe",
            "bet_datetime": "2024-11-09 20:55:30.016",
            "bet_num": 9,
            "betting_amount": 5.8,
            "price": 14.15,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 5.8,
        },
        {
            "bet_id": 171803,
            "customer_id": "e07d9c04-ed4a-4976-9bfe-9620bc121422",
            "bet_datetime": "not-a-date",
            "bet_num": 1,
            "betting_amount": 7.2,
            "price": 3.59,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 7.2,
        },
        {
            "bet_id": 90619,
            "customer_id": "e07d9c04-ed4a-4976-9bfe-9620bc121422",
            "bet_datetime": "2024-10-01 04:57:23.030",
            "bet_num": 2,
            "betting_amount": 33.5,
            "price": 17.42,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 33.5,
        },
        {
            "bet_id": 88440,
            "customer_id": "e07d9c04-ed4a-4976-9bfe-9620bc121422",
            "bet_datetime": "2024-10-24 11:22:12.043",
            "bet_num": 2,
            "betting_amount": 2.4,
            "price": 5.16,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 2.4,
        },
        {
            "bet_id": 295873,
            "customer_id": "e07d9c04-ed4a-4976-9bfe-9620bc121422",
            "bet_datetime": "2024-09-02 14:03:29.042",
            "bet_num": 0,
            "betting_amount": 3.8,
            "price": 8.97,
            "category": "sports",
            "stake_type": "cash",
            "bet_result": "no-return",
            "payout": 0.0,
            "return_for_entain": 3.8,
        },
    ]

    return pd.DataFrame(rows)


def build_expected_validation_report():
    return {
        "total_bets_input": 14,
        "total_valid_bets": 1,
        "total_invalid_bets": 13,
        "unique_customers_input": 3,
        "unique_customers_with_valid_bets": 1,
        "users_removed_all_bets_invalid": 2,
        "invalid_by_rule": {
            "duplicate_bet_id": 2,
            "invalid_bet_datetime": 1,
            "bet_num_not_positive_integer": 1,
            "duplicate_customer_bet_num": 2,
            "betting_amount_not_gt_0": 1,
            "price_not_gt_1": 1,
            "invalid_category": 1,
            "invalid_stake_type": 1,
            "invalid_bet_result": 1,
            "payout_mismatch": 2,
            "return_for_entain_mismatch": 2,
        },
        "first_20_window_health": {
            "window_definition": "raw bet_num between 1 and 20 inclusive",
            "users_with_at_least_1_invalid_bet_in_first_20": 3,
            "total_bets_in_first_20_window": 13,
            "invalid_bets_in_first_20_window": 12,
            "pct_invalid_bets_in_first_20_window": 12 / 13,
            "users_with_invalid_first_bet": 2,
        },
    }


def test_validation_report_matches_reference(tmp_path):
    output_dir = tmp_path / "validation_output"

    run_validation(None, output_dir, bets_df=build_invalid_bets_dataframe())

    with (output_dir / "validation_report.json").open(encoding="utf-8") as file:
        actual_report = json.load(file)

    expected_report = build_expected_validation_report()
    assert actual_report == expected_report


def test_invalid_output_contains_expected_values_and_multiple_reasons(tmp_path):
    output_dir = tmp_path / "validation_output"

    run_validation(None, output_dir, bets_df=build_invalid_bets_dataframe())

    invalid_bets = pd.read_csv(output_dir / "invalid_bets.csv")

    assert "expected_payout" in invalid_bets.columns
    assert "expected_return_for_entain" in invalid_bets.columns

    first_row = invalid_bets.loc[invalid_bets["bet_id"] == 100278].iloc[0]
    assert first_row["invalid_reasons"] == (
        "betting_amount_not_gt_0,payout_mismatch,return_for_entain_mismatch"
    )
