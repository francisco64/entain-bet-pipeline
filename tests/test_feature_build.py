from datetime import datetime, timezone

import pandas as pd

from bet_pipeline.build_features import build_customer_features


def test_feature_build_uses_only_valid_bets_inside_raw_first_20_window(tmp_path):
    valid_bets = pd.DataFrame(
        [
            {
                "bet_id": 1,
                "customer_id": "customer-1",
                "bet_datetime": "2024-01-01 10:00:00",
                "bet_num": 1,
                "betting_amount": 10.0,
                "price": 2.0,
                "category": "sports",
                "stake_type": "cash",
                "bet_result": "return",
                "payout": 20.0,
                "return_for_entain": -10.0,
            },
            {
                "bet_id": 2,
                "customer_id": "customer-1",
                "bet_datetime": "2024-01-02 10:00:00",
                "bet_num": 20,
                "betting_amount": 5.0,
                "price": 3.0,
                "category": "racing",
                "stake_type": "bonus",
                "bet_result": "return",
                "payout": 10.0,
                "return_for_entain": -10.0,
            },
            {
                "bet_id": 3,
                "customer_id": "customer-1",
                "bet_datetime": "2024-01-03 10:00:00",
                "bet_num": 21,
                "betting_amount": 100.0,
                "price": 5.0,
                "category": "racing",
                "stake_type": "cash",
                "bet_result": "return",
                "payout": 500.0,
                "return_for_entain": -400.0,
            },
        ]
    )

    valid_bets["bet_datetime"] = pd.to_datetime(valid_bets["bet_datetime"])
    valid_bets_path = tmp_path / "valid_bets.parquet"
    valid_bets.to_parquet(valid_bets_path, index=False)

    features = build_customer_features(valid_bets_path)
    row = features.iloc[0]

    assert row["bets_used"] == 2
    assert row["total_betting_amount"] == 15.0
    assert row["twentieth_bet_datetime"] == pd.Timestamp("2024-01-02 10:00:00")
    assert row["feature_generated_at"].tzinfo == timezone.utc
