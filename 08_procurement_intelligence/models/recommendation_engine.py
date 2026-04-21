"""
============================================================
 models/recommendation_engine.py
 Strategic Recommendations for Suppliers & Procurement Officials

 Uses the opportunity matrix + anomaly results to generate
 natural-language, actionable intelligence outputs.
============================================================
"""

import numpy as np
import pandas as pd
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import QUARTER_LABELS
from utils.feature_engineering import format_zar

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 1. Supplier-facing recommendations
# ─────────────────────────────────────────────────────────────

def generate_supplier_recommendations(
    matrix: pd.DataFrame,
    sector: str,
    available_capital: float = None,
    preferred_province: str = None,
    current_quarter: int = None,
    top_n: int = 5,
) -> list[dict]:
    """
    Generate ranked bid-strategy recommendations for a supplier.

    Parameters
    ----------
    matrix              : output of build_opportunity_matrix()
    sector              : supplier's primary sector (works/goods/services)
    available_capital   : max contract size supplier can handle (ZAR)
    preferred_province  : geographic preference (optional)
    current_quarter     : current SA fiscal quarter (1–4)
    top_n               : max recommendations to return
    """
    df = matrix[matrix["category"].str.lower() == sector.lower()].copy()

    if df.empty:
        return [{"message": f"No data found for sector: {sector}"}]

    # Filter by capital constraint
    if available_capital:
        df = df[df["median_value"] <= available_capital * 2]

    # Boost score for preferred province
    if preferred_province:
        df["score_adj"] = df["opportunity_score"] * df["province"].apply(
            lambda p: 1.3 if p.lower() == preferred_province.lower() else 1.0
        )
    else:
        df["score_adj"] = df["opportunity_score"]

    # Boost current quarter
    if current_quarter:
        df["score_adj"] *= df["quarter"].apply(
            lambda q: 1.2 if q == current_quarter else 1.0
        )

    df = df.sort_values("score_adj", ascending=False).head(top_n)

    recommendations = []
    for rank, (_, row) in enumerate(df.iterrows(), 1):
        timing = _timing_advice(int(row["quarter"]))
        cap_note = ""
        if available_capital and row["median_value"] > available_capital:
            cap_note = " ⚠ Median contract exceeds your capital — consider JV."

        rec = {
            "rank":              rank,
            "province":          row["province"],
            "sector":            row["category"],
            "quarter":           row["quarter_label"],
            "median_value":      row["median_value_fmt"],
            "total_market":      row["total_value_fmt"],
            "contract_count":    int(row["contract_count"]),
            "reliability":       str(row["reliability_tier"]),
            "opportunity_score": round(float(row["opportunity_score"]), 3),
            "pct_national":      f"{row['pct_of_national_spend']:.2f}%",
            "gauteng_warning":   bool(row["gauteng_flag"]),
            "timing_advice":     timing,
            "capital_note":      cap_note,
            "action":            _action_text(row, timing),
        }
        recommendations.append(rec)

    return recommendations


def _timing_advice(quarter: int) -> str:
    tips = {
        1: "Jan–Mar: Slow start — submit pipeline proposals and pre-qualifications.",
        2: "Apr–Jun: Post-budget release — strong contracting activity begins.",
        3: "Jul–Sep: PEAK quarter (August surge) — deploy full bid capacity.",
        4: "Oct–Dec: Year-end push — watch for accelerated awards; December dormant.",
    }
    return tips.get(quarter, "N/A")


def _action_text(row, timing: str) -> str:
    return (
        f"Target {row['province']} {row['category']} contracts in "
        f"{row['quarter_label']}. Expected median value: {row['median_value_fmt']}. "
        f"Market reliability: {row['reliability_tier']}. {timing}"
    )


# ─────────────────────────────────────────────────────────────
# 2. Procurement official recommendations
# ─────────────────────────────────────────────────────────────

def generate_official_recommendations(
    df_flagged: pd.DataFrame,
    anomaly_summary: dict,
) -> list[dict]:
    """
    Generate recommendations for procurement officials based on
    anomaly detection results.
    """
    recs = []

    # High severity contracts
    high_risk = df_flagged[df_flagged["anomaly_severity"] >= 3]
    if not high_risk.empty:
        recs.append({
            "priority":    "URGENT",
            "category":    "Contract Review",
            "finding":     f"{len(high_risk)} high-risk contracts flagged.",
            "detail":      f"Total exposed value: {format_zar(high_risk['contract_value'].sum())}.",
            "action":      "Initiate immediate procurement review. Cross-check tender documentation and supplier BBB-EE certificates.",
        })

    # Supplier concentration
    conc = df_flagged[df_flagged["anomaly_flag"].str.startswith("FLAG 6")]
    if not conc.empty:
        cats  = conc["category"].unique().tolist()
        recs.append({
            "priority": "HIGH",
            "category": "Supplier Concentration",
            "finding":  f"Concentration detected in: {', '.join(cats)}.",
            "detail":   "Single suppliers control >30% of category spend.",
            "action":   "Review supply chain diversity strategy. Consider supplier development programmes.",
        })

    # Restricted method misuse
    flag1 = df_flagged[df_flagged["anomaly_flag"].str.startswith("FLAG 1")]
    if not flag1.empty:
        recs.append({
            "priority": "HIGH",
            "category": "Procurement Method Compliance",
            "finding":  f"{len(flag1)} restricted-method awards above R15M threshold.",
            "detail":   f"Total value: {format_zar(flag1['contract_value'].sum())}.",
            "action":   "Verify compliance with PFMA/MFMA thresholds. Escalate to Treasury.",
        })

    # Quarterly spikes
    spikes = df_flagged[df_flagged["anomaly_flag"].str.startswith("FLAG 7")]
    if not spikes.empty:
        provs = spikes["province"].unique().tolist()
        recs.append({
            "priority": "MEDIUM",
            "category": "Budget Execution Patterns",
            "finding":  f"Unusual quarterly spend spikes in: {', '.join(provs)}.",
            "detail":   "Spend >2× the provincial quarterly average — possible year-end rush.",
            "action":   "Review if spend aligns with approved MTEF budget. Flag for internal audit.",
        })

    # New supplier large awards
    flag3 = df_flagged[df_flagged["anomaly_flag"].str.startswith("FLAG 3")]
    if not flag3.empty:
        recs.append({
            "priority": "HIGH",
            "category": "New Supplier Due Diligence",
            "finding":  f"{len(flag3)} large awards to suppliers with no prior government history.",
            "detail":   f"Average award: {format_zar(flag3['contract_value'].mean())}.",
            "action":   "Conduct enhanced due diligence. Verify registration, tax clearance, and BEE standing.",
        })

    if not recs:
        recs.append({
            "priority": "LOW",
            "category": "General",
            "finding":  "No critical anomalies detected in this dataset.",
            "detail":   "Continue routine monitoring.",
            "action":   "Maintain current controls.",
        })

    return recs


# ─────────────────────────────────────────────────────────────
# 3. Policy analyst brief
# ─────────────────────────────────────────────────────────────

def generate_policy_brief(
    df: pd.DataFrame,
    matrix: pd.DataFrame,
    anomaly_summary: dict,
) -> dict:
    """
    High-level procurement intelligence brief for policy analysts.
    """
    total_spend     = df["contract_value"].sum()
    median_contract = df["contract_value"].median()
    mean_contract   = df["contract_value"].mean()
    skew_ratio      = mean_contract / median_contract

    top_province    = (
        df.groupby("province")["contract_value"].sum()
          .sort_values(ascending=False).index[0]
    )
    top_sector      = (
        df.groupby("category")["contract_value"].sum()
          .sort_values(ascending=False).index[0]
    )

    open_pct = (
        df["method"].str.lower().eq("open").mean() * 100
    )

    return {
        "executive_summary": {
            "total_spend":        format_zar(total_spend),
            "total_contracts":    f"{len(df):,}",
            "median_contract":    format_zar(median_contract),
            "mean_contract":      format_zar(mean_contract),
            "mean_median_ratio":  f"{skew_ratio:.0f}×",
            "open_tender_pct":    f"{open_pct:.1f}%",
            "top_province":       top_province,
            "top_sector":         top_sector,
        },
        "key_findings": [
            f"Procurement appears open ({open_pct:.0f}% open tender) but value is "
            f"highly concentrated — mean/median ratio of {skew_ratio:.0f}×.",
            f"{top_province} leads in spend share; Gauteng has high volume but below-average contract value.",
            "August is peak tender month; December–January is dormant.",
            f"Anomaly rate: {anomaly_summary['flag_rate_pct']}% — "
            f"{anomaly_summary['high_severity_count']} high-severity flags.",
        ],
        "recommendations": [
            "Publish province-quarter spending forecasts to improve market access for SMMEs.",
            "Investigate mega-contract outliers and restricted-method awards above legal thresholds.",
            "Develop supplier development pipeline targeting high-opportunity cells (KZN Works Q2 etc.).",
            "Digitise bid evaluation to reduce anomalous single-source awards.",
        ],
    }


