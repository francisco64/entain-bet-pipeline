REQUIRED_COLUMNS = [
    "bet_id",
    "customer_id",
    "bet_datetime",
    "bet_num",
    "betting_amount",
    "price",
    "category",
    "stake_type",
    "bet_result",
    "payout",
    "return_for_entain",
]

NUMERIC_COLUMNS = [
    "bet_id",
    "bet_num",
    "betting_amount",
    "price",
    "payout",
    "return_for_entain",
]

ALLOWED_CATEGORIES = {"sports", "racing"}
ALLOWED_STAKE_TYPES = {"cash", "bonus"}
ALLOWED_BET_RESULTS = {"return", "no-return"}

FIRST_20_MAX_BET_NUM = 20
FLOAT_TOLERANCE = 1e-6
