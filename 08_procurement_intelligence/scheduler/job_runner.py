"""
============================================================
 scheduler/job_runner.py
 Automated Pipeline Scheduler

 Runs the full intelligence pipeline on a schedule:
 • Weekly: full retrain + anomaly refresh + Excel report
 • Daily:  anomaly-only refresh (fast, no retraining)
 • Ad-hoc: triggered by API call or CLI

 Run as a service:
    python scheduler/job_runner.py
 Or trigger manually:
    python scheduler/job_runner.py --run-now full
============================================================
"""

import sys
import argparse
import logging
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    print("APScheduler not installed. Install with: pip install apscheduler")
    print("Running in manual mode only.\n")

from config import REPORT_DIR, DATA_DIR, MODEL_DIR

# Ensure the report directory exists for the log file
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s --- %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(REPORT_DIR / "scheduler.log"),
    ]
)

logger = logging.getLogger("scheduler")


# ─────────────────────────────────────────────────────────────
# Common data loader – always uses the real cleaned CSV
# ─────────────────────────────────────────────────────────────

def _load_real_data():
    """
    Load the real, filtered dataset (master_with_anomalies.csv).
    Exits the job if the file is missing.
    """
    csv = DATA_DIR / "master_with_anomalies.csv"
    if not csv.exists():
        logger.error("Real data file missing: %s", csv)
        raise FileNotFoundError(f"Required data file not found: {csv}")
    import pandas as pd
    return pd.read_csv(csv, parse_dates=["tender_date"], low_memory=False)


# ─────────────────────────────────────────────────────────────
# Job definitions
# ─────────────────────────────────────────────────────────────

def job_full_pipeline():
    """Weekly full pipeline: reload data, retrain all models, rebuild matrix, anomaly scan, Excel report."""
    logger.info("=" * 60)
    logger.info("JOB START --- Full Pipeline Retrain")
    t0 = time.time()
    status = {"job": "full_pipeline", "started": datetime.now().isoformat()}

    try:
        # Load real data (no fallback)
        df = _load_real_data()

        # Anomaly pipeline
        from models.anomaly_detector import run_full_anomaly_pipeline, anomaly_summary_report
        df = run_full_anomaly_pipeline(df)
        anomaly_summary = anomaly_summary_report(df)

        # Opportunity matrix
        from models.opportunity_matrix import build_opportunity_matrix, save_matrix
        matrix = build_opportunity_matrix(df)
        save_matrix(matrix)

        # Retrain models
        from models.value_forecaster import train_all_models, print_model_comparison
        results = train_all_models(df)
        comparison = print_model_comparison(results)
        comparison.to_csv(REPORT_DIR / "model_comparison.csv", index=False)

        # Excel report
        from reports.excel_reporter import generate_excel_report
        from models.recommendation_engine import generate_policy_brief
        brief = generate_policy_brief(df, matrix, anomaly_summary)
        excel_path = generate_excel_report(df, matrix, brief)

        status.update({
            "completed": datetime.now().isoformat(),
            "duration_seconds": round(time.time() - t0, 1),
            "records_processed": len(df),
            "anomalies_found": anomaly_summary["total_flagged"],
            "flag_rate_pct": anomaly_summary["flag_rate_pct"],
            "excel_report": str(excel_path),
            "result": "success"
        })
        logger.info("Full pipeline complete in %.1fs", status['duration_seconds'])

    except Exception as e:
        logger.error("Full pipeline FAILED: %s", e, exc_info=True)
        status.update({"result": "failed", "error": str(e)})
    _save_run_log(status)
    return status


def job_anomaly_refresh():
    """Daily anomaly refresh (no retraining)."""
    logger.info("=" * 60)
    logger.info("JOB START --- Daily Anomaly Refresh")
    t0 = time.time()
    status = {"job": "anomaly_refresh", "started": datetime.now().isoformat()}

    try:
        import pandas as pd
        from models.anomaly_detector import run_full_anomaly_pipeline, anomaly_summary_report

        # Load real data
        df = _load_real_data()

        df = run_full_anomaly_pipeline(df)
        summary = anomaly_summary_report(df)

        flagged = df[df["anomaly_flag"] != "Normal"]
        flagged.to_csv(REPORT_DIR / "flagged_contracts_daily.csv", index=False)
        with open(REPORT_DIR / "anomaly_summary_daily.json", "w") as f:
            json.dump({k: v for k, v in summary.items() if k != "top_flagged_contracts"},
                      f, indent=2, default=str)

        status.update({
            "completed": datetime.now().isoformat(),
            "duration_seconds": round(time.time() - t0, 1),
            "total_contracts": summary["total_records"],
            "flagged": summary["total_flagged"],
            "high_severity": summary["high_severity_count"],
            "result": "success"
        })
        logger.info("Anomaly refresh done in %.1fs", status['duration_seconds'])

    except Exception as e:
        logger.error("Anomaly refresh FAILED: %s", e, exc_info=True)
        status.update({"result": "failed", "error": str(e)})
    _save_run_log(status)
    return status


def job_opportunity_matrix_refresh():
    """Mid‑week opportunity matrix rebuild."""
    logger.info("JOB START --- Opportunity Matrix Refresh")
    t0 = time.time()
    status = {"job": "matrix_refresh", "started": datetime.now().isoformat()}

    try:
        import pandas as pd
        from models.opportunity_matrix import build_opportunity_matrix, save_matrix
        from models.anomaly_detector import run_full_anomaly_pipeline

        # Load real data
        df = _load_real_data()

        df = run_full_anomaly_pipeline(df)
        matrix = build_opportunity_matrix(df)
        save_matrix(matrix)

        status.update({
            "completed": datetime.now().isoformat(),
            "duration_seconds": round(time.time() - t0, 1),
            "matrix_cells": len(matrix),
            "result": "success"
        })
        logger.info("Matrix refresh done: %d cells in %.1fs", len(matrix), status['duration_seconds'])

    except Exception as e:
        logger.error("Matrix refresh FAILED: %s", e, exc_info=True)
        status.update({"result": "failed", "error": str(e)})
    _save_run_log(status)
    return status


# ─────────────────────────────────────────────────────────────
# Run log
# ─────────────────────────────────────────────────────────────

def _save_run_log(status: dict):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    log_file = REPORT_DIR / "run_history.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(status, default=str) + "\n")


def get_run_history(last_n: int = 20) -> list:
    log_file = REPORT_DIR / "run_history.jsonl"
    if not log_file.exists():
        return []
    lines = log_file.read_text().strip().split("\n")
    return [json.loads(l) for l in lines[-last_n:] if l.strip()]


# ─────────────────────────────────────────────────────────────
# Scheduler setup
# ─────────────────────────────────────────────────────────────

def start_scheduler():
    if not HAS_SCHEDULER:
        logger.error("APScheduler not installed. Cannot start scheduler.")
        return

    scheduler = BlockingScheduler(timezone="Africa/Johannesburg")
    scheduler.add_job(
        job_full_pipeline,
        trigger=CronTrigger(day_of_week="sun", hour=2, minute=0),
        id="full_pipeline_weekly",
        name="Weekly Full Pipeline Retrain",
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        job_anomaly_refresh,
        trigger=CronTrigger(day_of_week="mon-sat", hour=6, minute=0),
        id="anomaly_daily",
        name="Daily Anomaly Refresh",
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        job_opportunity_matrix_refresh,
        trigger=CronTrigger(day_of_week="wed", hour=8, minute=0),
        id="matrix_midweek",
        name="Mid-week Opportunity Matrix Refresh",
        misfire_grace_time=1800,
    )

    logger.info("Scheduler started with 3 jobs:")
    for job in scheduler.get_jobs():
        logger.info(" • %s (next: %s)", job.name, job.next_run_time)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procurement Intelligence Scheduler")
    parser.add_argument("--run-now", choices=["full", "anomaly", "matrix"],
                        help="Run a job immediately instead of scheduling")
    parser.add_argument("--history", action="store_true",
                        help="Print last 10 run logs")
    args = parser.parse_args()

    if args.history:
        history = get_run_history(10)
        if not history:
            print("No run history found.")
        else:
            print("\nLast 10 pipeline runs:")
            print("-" * 70)
            for run in history:
                icon = "✅" if run.get("result") == "success" else "❌"
                print(f"{icon} [{run.get('started','')[:19]}] {run.get('job','')} --- "
                    f"{run.get('result','')} ({run.get('duration_seconds','?')}s)")
        sys.exit(0)

    if args.run_now == "full":
        job_full_pipeline()
    elif args.run_now == "anomaly":
        job_anomaly_refresh()
    elif args.run_now == "matrix":
        job_opportunity_matrix_refresh()
    else:
        logger.info("Starting scheduled runner (Ctrl+C to stop)...")
        start_scheduler()