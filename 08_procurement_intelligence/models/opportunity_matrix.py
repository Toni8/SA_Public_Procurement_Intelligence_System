""" opportunity matrix engine - province/quarter/sector scoring
============================================================
 models/opportunity_matrix.py
 Province × Quarter × Sector Opportunity Scoring Engine

 EDA insight: Geographic targeting (province + quarter)
 is the highest-value intelligence signal in this dataset.
 KZN Works Q2 is the canonical high-confidence example.
============================================================
"""

import numpy as np
import pandas as pd
import joblib
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import MIN_SAMPLE_SIZE, MODEL_DIR, QUARTER_LABELS

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 1. Core scoring function
# ─────────────────────────────────────────────────────────────

def build_opportunity_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the province × quarter × sector opportunity matrix.
    """
    required = ["province", "category", "quarter", "contract_value"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    # Force contract_value to float
    df = df.copy()
    df["contract_value"] = pd.to_numeric(df["contract_value"], errors="coerce").astype(float)
    df = df[df["contract_value"] > 0]

    grp = (
        df.groupby(["province", "category", "quarter"], observed=True)
        .agg(
            contract_count = ("contract_value", "count"),
            median_value   = ("contract_value", "median"),
            mean_value     = ("contract_value", "mean"),
            total_value    = ("contract_value", "sum"),
            std_value      = ("contract_value", "std"),
        )
        .reset_index()
    )

    # Convert all numeric columns to float
    for col in ["median_value", "mean_value", "total_value", "std_value"]:
        grp[col] = grp[col].astype(float)

    # Reliability tier (categorical)
    grp["reliability_tier"] = pd.cut(
        grp["contract_count"],
        bins=[0, 1, 4, 9, np.inf],
        labels=["Insufficient Data", "Caution", "Reliable", "High Confidence"],
        right=True,
    )

    # Map weights – convert to float numpy arrays immediately
    rw_map = {"High Confidence": 1.0, "Reliable": 0.6, "Caution": 0.3, "Insufficient Data": 0.0}
    rel_w = grp["reliability_tier"].map(rw_map).astype(float).values

    sw_map = {1: 0.85, 2: 1.05, 3: 1.20, 4: 0.95}
    seas_w = grp["quarter"].map(sw_map).astype(float).values

    # Median values as numpy array
    medians = grp["median_value"].values

    # Opportunity score – pure numpy arithmetic
    grp["opportunity_score"] = (np.log1p(medians) * rel_w * seas_w).round(4)

    # Derived columns
    total_national = grp["total_value"].sum()
    grp["pct_of_national_spend"] = ((grp["total_value"] / total_national) * 100).round(3)

    grp["value_rank"] = grp["opportunity_score"].rank(ascending=False).astype(int)
    grp["quarter_label"] = grp["quarter"].map(QUARTER_LABELS)
    grp["gauteng_flag"] = (grp["province"].str.lower() == "gauteng").astype(int)

    # Formatting
    def _fmt(v):
        if pd.isna(v): return "N/A"
        if v >= 1e9: return f"R{v/1e9:.2f}B"
        if v >= 1e6: return f"R{v/1e6:.2f}M"
        if v >= 1e3: return f"R{v/1e3:.1f}K"
        return f"R{v:,.0f}"

    grp["median_value_fmt"] = grp["median_value"].apply(_fmt)
    grp["total_value_fmt"]  = grp["total_value"].apply(_fmt)

    matrix = (
        grp[grp["reliability_tier"] != "Insufficient Data"]
        .sort_values("opportunity_score", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(f"Opportunity matrix: {len(matrix)} province×category×quarter cells.")
    return matrix


# ─────────────────────────────────────────────────────────────
# 2. Top-N opportunities for a given supplier profile
# ─────────────────────────────────────────────────────────────

def get_top_opportunities(
    matrix: pd.DataFrame,
    sector: str = None,
    province: str = None,
    quarter: int = None,
    top_n: int = 10,
    min_contracts: int = 5,
    exclude_gauteng: bool = False,
) -> pd.DataFrame:
    """
    Filter the opportunity matrix and return top_n recommendations.
    """
    df = matrix.copy()

    if sector:
        df = df[df["category"].str.lower() == sector.lower()]
    if province:
        df = df[df["province"].str.lower() == province.lower()]
    if quarter:
        df = df[df["quarter"] == quarter]
    if exclude_gauteng:
        df = df[df["gauteng_flag"] == 0]

    df = df[df["contract_count"] >= min_contracts]

    return (
        df.sort_values("opportunity_score", ascending=False)
          .head(top_n)
          [["value_rank", "province", "category", "quarter_label",
            "contract_count", "median_value_fmt", "total_value_fmt",
            "reliability_tier", "opportunity_score", "pct_of_national_spend",
            "gauteng_flag"]]
          .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────
# 3. Province × Quarter heat‑map pivot
# ─────────────────────────────────────────────────────────────

def build_heatmap_pivot(
    matrix: pd.DataFrame,
    value_col: str = "median_value",
    sector: str = None
) -> pd.DataFrame:
    """
    Return a province × quarter pivot table (for visualisation).
    value_col: 'median_value', 'opportunity_score', 'total_value'
    """
    df = matrix.copy()
    if sector:
        df = df[df["category"].str.lower() == sector.lower()]

    pivot = df.pivot_table(
        index="province",
        columns="quarter",
        values=value_col,
        aggfunc="median",
        fill_value=0,
    )
    pivot.columns = [QUARTER_LABELS.get(c, f"Q{c}") for c in pivot.columns]
    return pivot.sort_values(list(pivot.columns)[0], ascending=False)


# ─────────────────────────────────────────────────────────────
# 4. Save / load matrix
# ─────────────────────────────────────────────────────────────

def save_matrix(matrix: pd.DataFrame, name: str = "opportunity_matrix"):
    path = MODEL_DIR / f"{name}.pkl"
    joblib.dump(matrix, path)
    csv_path = MODEL_DIR / f"{name}.csv"
    matrix.to_csv(csv_path, index=False)
    logger.info(f"Matrix saved: {path} + {csv_path}")


def load_matrix(name: str = "opportunity_matrix") -> pd.DataFrame:
    path = MODEL_DIR / f"{name}.pkl"
    if path.exists():
        return joblib.load(path)
    csv_path = MODEL_DIR / f"{name}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Opportunity matrix not found at {path}")