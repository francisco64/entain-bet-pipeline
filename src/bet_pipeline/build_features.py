from pathlib import Path
from datetime import datetime, timezone

import duckdb

from bet_pipeline.schema import FIRST_20_MAX_BET_NUM


FEATURE_QUERY = """
WITH first_20_valid_bets AS (
    SELECT
        customer_id,
        bet_datetime,
        bet_num,
        betting_amount,
        price,
        category,
        stake_type,
        bet_result,
        payout,
        return_for_entain
    FROM '{valid_bets_path}'
    WHERE bet_num >= 1 AND bet_num <= {first_20_max_bet_num}
),
ordered_bets AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY bet_num
        ) AS valid_bet_order
    FROM first_20_valid_bets
)
SELECT
    customer_id,
    MAX(CASE WHEN valid_bet_order = 1 THEN bet_datetime END) AS first_bet_datetime,
    MAX(CASE WHEN bet_num = {first_20_max_bet_num} THEN bet_datetime END) AS twentieth_bet_datetime,
    COUNT(*) AS bets_used,
    SUM(betting_amount) AS total_betting_amount,
    AVG(betting_amount) AS mean_betting_amount,
    AVG(price) AS mean_price,
    AVG(CASE WHEN category = 'racing' THEN 1.0 ELSE 0.0 END) AS pct_racing,
    AVG(CASE WHEN stake_type = 'cash' THEN 1.0 ELSE 0.0 END) AS pct_cash,
    AVG(CASE WHEN bet_result = 'return' THEN 1.0 ELSE 0.0 END) AS pct_return,
    SUM(payout) AS total_payout,
    SUM(return_for_entain) AS total_return_for_entain
FROM ordered_bets
GROUP BY customer_id
ORDER BY customer_id
"""


def build_customer_features(valid_bets_path):
    valid_bets_path = str(valid_bets_path).replace("'", "''")
    query = FEATURE_QUERY.format(
        valid_bets_path=valid_bets_path,
        first_20_max_bet_num=FIRST_20_MAX_BET_NUM,
    )

    connection = duckdb.connect()
    try:
        features = connection.execute(query).df()
    finally:
        connection.close()

    features["feature_generated_at"] = datetime.now(timezone.utc)
    return features


def run_feature_build(valid_bets_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    features = build_customer_features(valid_bets_path)
    features.to_parquet(output_dir / "customer_features.parquet", index=False)

    return features
