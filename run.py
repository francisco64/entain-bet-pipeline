import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from bet_pipeline.validate import run_validation
from bet_pipeline.build_features import run_feature_build

run_validation("bets.csv", "outputs/validation")
run_feature_build("outputs/validation/valid_bets.parquet", "outputs/features")
