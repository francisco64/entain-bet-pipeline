import argparse
import shutil
import tempfile
from pathlib import Path

from bet_pipeline.build_features import run_feature_build
from bet_pipeline.validate import run_validation


def build_parser():
    parser = argparse.ArgumentParser(prog="bet-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--input", required=True)
    validate_parser.add_argument("--output", required=True)

    feature_parser = subparsers.add_parser("build-features")
    feature_parser.add_argument("--input", required=True)
    feature_parser.add_argument("--output", required=True)

    return parser


def run_build_features_from_csv(input_path, output_dir):
    output_dir = Path(output_dir)
    temp_validation_dir = Path(tempfile.mkdtemp(prefix="bet_pipeline_validation_"))

    try:
        run_validation(input_path, temp_validation_dir)
        run_feature_build(temp_validation_dir / "valid_bets.parquet", output_dir)
    finally:
        shutil.rmtree(temp_validation_dir, ignore_errors=True)


def main():
    args = build_parser().parse_args()

    if args.command == "validate":
        run_validation(args.input, args.output)


    if args.command == "build-features":
        run_build_features_from_csv(args.input, args.output)
