"""
============================================================
 train_pipeline.py
 MASTER TRAINING SCRIPT — Run this first after DB is populated.

 Steps:
   1. Load master dataset from MySQL
   2. Run anomaly detection pipeline
   3. Build opportunity matrix
   4. Train all ML models (RF, XGB, LGB, Ridge, Ensemble)
   5. Save models + matrix to /models/
   6. Print comparison report
   7. Export summary CSV reports

 Usage:
   python train_pipeline.py [--sample N] [--skip-db]

 --sample N    : Use N random rows (for testing without full DB)
 --skip-db     : Load from data/sample.csv instead of MySQL
============================================================
"""

import argparse
import logging
import sys
import json
import time
import pandas as pd
import numpy as np
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s — %(message)s",
)
logger = logging.getLogger("train_pipeline")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, MODEL_DIR, REPORT_DIR

DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--sample", type=int, default=None,
                   help="Limit rows (testing). E.g. --sample 5000")
    p.add_argument("--skip-db", action="store_true",
                   help="Load from data/sample.csv instead of MySQL")
    return p.parse_args()


def load_data(args) -> pd.DataFrame:
    if args.skip_db:
        csv = DATA_DIR / "sample.csv"
        if not csv.exists():
            logger.error(f"Sample CSV not found: {csv}")
            sys.exit(1)
        df = pd.read_csv(csv, parse_dates=["tender_date"])
        logger.info(f"Loaded from CSV: {len(df):,} rows.")
    else:
        from utils.db_loader import load_master_df
        df = load_master_df()

    if args.sample:
        df = df.sample(min(args.sample, len(df)), random_state=42)
        logger.info(f"Sampled to {len(df):,} rows.")
    return df


def main():
    args   = parse_args()
    t0     = time.time()

    # ── 1. Load ────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1: Loading data")
    df = load_data(args)
    logger.info(f"  Rows: {len(df):,}  |  Columns: {df.shape[1]}")

    # ── 2. Anomaly detection ───────────────────────────────
    logger.info("STEP 2: Anomaly detection")
    from models.anomaly_detector import run_full_anomaly_pipeline, anomaly_summary_report
    df = run_full_anomaly_pipeline(df)
    anomaly_summary = anomaly_summary_report(df)
    logger.info(
        f"  Flagged: {anomaly_summary['total_flagged']} / {anomaly_summary['total_records']} "
        f"({anomaly_summary['flag_rate_pct']}%)"
    )

    # Save anomaly report
    anomaly_path = REPORT_DIR / "anomaly_summary.json"
    with open(anomaly_path, "w") as f:
        # Convert non-serialisable values
        json.dump(
            {k: (int(v) if isinstance(v, (np.integer,)) else
                 float(v) if isinstance(v, (np.floating,)) else v)
             for k, v in anomaly_summary.items()
             if k != "top_flagged_contracts"},
            f, indent=2
        )
    flagged_df = df[df["anomaly_flag"] != "Normal"]
    flagged_df.to_csv(REPORT_DIR / "flagged_contracts.csv", index=False)
    logger.info(f"  Anomaly reports saved → {REPORT_DIR}")

    # ── 3. Opportunity matrix ──────────────────────────────
    logger.info("STEP 3: Building opportunity matrix")
    from models.opportunity_matrix import build_opportunity_matrix, save_matrix
    matrix = build_opportunity_matrix(df)
    save_matrix(matrix)
    logger.info(f"  Matrix cells: {len(matrix)}")

    # Top 20 opportunities to CSV
    top20 = matrix.head(20)[
        ["province", "category", "quarter_label",
         "contract_count", "median_value_fmt", "total_value_fmt",
         "reliability_tier", "opportunity_score"]
    ]
    top20.to_csv(REPORT_DIR / "top_opportunities.csv", index=False)
    logger.info(f"  Top 20 opportunities saved.")

    # ── 4. Train ML models ─────────────────────────────────
    logger.info("STEP 4: Training ML models")
    from models.value_forecaster import train_all_models, print_model_comparison
    results = train_all_models(df)
    comparison_df = print_model_comparison(results)
    comparison_df.to_csv(REPORT_DIR / "model_comparison.csv", index=False)
    logger.info(f"  Models saved → {MODEL_DIR}")

    # ── 5. Recommendations ────────────────────────────────
    logger.info("STEP 5: Generating strategic recommendations")
    from models.recommendation_engine import (
        generate_supplier_recommendations,
        generate_official_recommendations,
        generate_policy_brief,
    )

    # Sample supplier rec — KZN Works (canonical high-signal from EDA)
    sup_recs = generate_supplier_recommendations(
        matrix, sector="Works",
        preferred_province="KwaZulu-Natal",
        current_quarter=2, top_n=5,
    )
    with open(REPORT_DIR / "supplier_recommendations_sample.json", "w") as f:
        json.dump(sup_recs, f, indent=2, default=str)

    off_recs = generate_official_recommendations(df, anomaly_summary)
    with open(REPORT_DIR / "official_recommendations.json", "w") as f:
        json.dump(off_recs, f, indent=2, default=str)

    brief = generate_policy_brief(df, matrix, anomaly_summary)
    with open(REPORT_DIR / "policy_brief.json", "w") as f:
        json.dump(brief, f, indent=2, default=str)

    # ── 6. Final summary ──────────────────────────────────
    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.1f}s")
    logger.info(f"  Reports  → {REPORT_DIR}")
    logger.info(f"  Models   → {MODEL_DIR}")
    logger.info("=" * 60)

    print("\n📊 POLICY BRIEF SUMMARY")
    print("─" * 50)
    for k, v in brief["executive_summary"].items():
        print(f"  {k:25s}: {v}")
    print("\n🔍 KEY FINDINGS:")
    for finding in brief["key_findings"]:
        print(f"  • {finding}")
    print("\n🏆 TOP 5 OPPORTUNITIES:")
    for _, row in top20.head(5).iterrows():
        print(f"  [{row['reliability_tier']:15s}] {row['province']:20s} | {row['category']:10s} | "
            f"{row['quarter_label']} | Median: {row['median_value_fmt']}")
    print()


if __name__ == "__main__":
    main()


