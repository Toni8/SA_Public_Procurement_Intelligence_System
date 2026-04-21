""" anomaly detention engine for governance red flags
============================================================
 models/anomaly_detector.py
 Governance Red-Flag & Anomaly Detection Engine

 EDA-derived rules:
   FLAG 1  — Restricted method above R15M
   FLAG 2  — Non-open mega contract above R100M
   FLAG 3  — Single large award to supplier with no prior history (>R50M)
   FLAG 4  — Mega contract above R200M (any method)
   FLAG 5  — Statistical outlier (Z-score > 3 on log_value within group)
   FLAG 6  — Supplier concentration (top supplier >30% of category spend)
   FLAG 7  — Unusual quarterly spike (>2× rolling quarterly average)
============================================================
"""

import numpy as np
import pandas as pd
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import ANOMALY_THRESHOLDS

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 1. Rule-based flags (direct from BI SQL logic)
# ─────────────────────────────────────────────────────────────

RESTRICTED_METHODS = {"direct", "limited", "selective", "restricted"}


def apply_rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add anomaly_flag column based on business rules from the SQL BI layer.
    Input must have: method, contract_value, supplier_prior_awards, province, category
    """
    T = ANOMALY_THRESHOLDS
    flags = []

    for _, row in df.iterrows():
        method   = str(row.get("method", "")).lower()
        value    = float(row.get("contract_value", 0))
        prior    = int(row.get("supplier_prior_awards", 1))

        f = "Normal"
        if method in RESTRICTED_METHODS and value > T["restricted_above"]:
            f = f"FLAG 1 — Restricted method above R{T['restricted_above']/1e6:.0f}M"
        elif value > T["nonopen_mega"] and method not in ("open",):
            f = f"FLAG 2 — Non-open mega contract >R{T['nonopen_mega']/1e6:.0f}M"
        elif value > T["new_supplier_large"] and prior <= 1:
            f = f"FLAG 3 — Single large award, no prior history (>R{T['new_supplier_large']/1e6:.0f}M)"
        elif value > T["mega_contract"]:
            f = f"FLAG 4 — Mega contract >R{T['mega_contract']/1e6:.0f}M"

        flags.append(f)

    df = df.copy()
    df["anomaly_flag"] = flags
    return df


# ─────────────────────────────────────────────────────────────
# 2. Statistical outlier detection
# ─────────────────────────────────────────────────────────────

def flag_statistical_outliers(
    df: pd.DataFrame,
    group_cols: list = None,
    z_cutoff: float = None,
) -> pd.DataFrame:
    """
    FLAG 5: Z-score based outlier detection on log_value,
    computed within each (province, category) group.
    """
    if group_cols is None:
        group_cols = ["province", "category"]
    if z_cutoff is None:
        z_cutoff = ANOMALY_THRESHOLDS["zscore_cutoff"]

    df = df.copy()
    if "log_value" not in df.columns:
        df["log_value"] = np.log1p(df["contract_value"])

    def _zscore(series):
        mu, sigma = series.mean(), series.std()
        if sigma == 0 or np.isnan(sigma):
            return pd.Series(0.0, index=series.index)
        return (series - mu).abs() / sigma

    df["log_zscore"] = (
        df.groupby(group_cols)["log_value"]
          .transform(_zscore)
    )
    df["is_statistical_outlier"] = (df["log_zscore"] > z_cutoff).astype(int)
    df.loc[df["is_statistical_outlier"] == 1, "anomaly_flag"] = (
        "FLAG 5 — Statistical outlier (Z=" +
        df.loc[df["is_statistical_outlier"] == 1, "log_zscore"].round(1).astype(str) + ")"
    )
    return df


# ─────────────────────────────────────────────────────────────
# 3. Supplier concentration flag
# ─────────────────────────────────────────────────────────────

def flag_supplier_concentration(
    df: pd.DataFrame,
    concentration_threshold: float = 0.30,
) -> pd.DataFrame:
    """
    FLAG 6: A single supplier holding >30% of category spend.
    EDA insight: Restricted-method contracts despite <2% volume show
    higher average values → supplier lock-in signal.
    """
    df = df.copy()
    if "supplier_name" not in df.columns:
        return df

    cat_total = df.groupby("category")["contract_value"].sum().rename("cat_total")
    sup_share = (
        df.groupby(["category", "supplier_name"])["contract_value"]
          .sum()
          .reset_index()
          .merge(cat_total, on="category")
    )
    sup_share["share"] = sup_share["contract_value"] / sup_share["cat_total"]
    concentrated = sup_share[sup_share["share"] > concentration_threshold][
        ["category", "supplier_name", "share"]
    ]

    if not concentrated.empty:
        conc_map = {}
        for _, row in concentrated.iterrows():
            conc_map[(row["category"], row["supplier_name"])] = row["share"]

        def _flag_conc(row):
            key = (row.get("category"), row.get("supplier_name"))
            if key in conc_map:
                pct = conc_map[key] * 100
                return f"FLAG 6 — Supplier concentration {pct:.0f}% of {key[0]} spend"
            return row.get("anomaly_flag", "Normal")

        df["anomaly_flag"] = df.apply(_flag_conc, axis=1)

    return df


# ─────────────────────────────────────────────────────────────
# 4. Quarterly spike detection
# ─────────────────────────────────────────────────────────────

def flag_quarterly_spikes(
    df: pd.DataFrame,
    spike_multiplier: float = 2.0,
) -> pd.DataFrame:
    """
    FLAG 7: Total spend in a province-quarter cell exceeds
    2× the rolling average of surrounding quarters.
    """
    df = df.copy()
    if "quarter" not in df.columns or "province" not in df.columns:
        return df

    q_total = (
        df.groupby(["province", "quarter"])["contract_value"]
          .sum()
          .reset_index()
          .rename(columns={"contract_value": "q_total"})
    )
    q_mean = (
        q_total.groupby("province")["q_total"]
               .mean()
               .rename("prov_mean")
    )
    q_total = q_total.merge(q_mean, on="province")
    spikes  = q_total[q_total["q_total"] > spike_multiplier * q_total["prov_mean"]]

    spike_keys = set(zip(spikes["province"], spikes["quarter"]))
    df["is_quarterly_spike"] = df.apply(
        lambda r: int((r.get("province"), r.get("quarter")) in spike_keys), axis=1
    )
    spike_mask = df["is_quarterly_spike"] == 1
    df.loc[spike_mask, "anomaly_flag"] = (
        "FLAG 7 — Quarterly spend spike (>2× provincial average)"
    )
    return df


# ─────────────────────────────────────────────────────────────
# 5. Full anomaly pipeline
# ─────────────────────────────────────────────────────────────

def run_full_anomaly_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all anomaly detectors in sequence.
    Returns the input df with additional columns:
      anomaly_flag, log_zscore, is_statistical_outlier,
      is_quarterly_spike, anomaly_severity
    """
    logger.info("Running anomaly detection pipeline...")

    df = apply_rule_flags(df)
    df = flag_statistical_outliers(df)
    df = flag_supplier_concentration(df)
    df = flag_quarterly_spikes(df)

    # Severity score (higher = more concerning)
    severity_map = {
        "Normal": 0,
        "FLAG 7": 1,
        "FLAG 1": 2,
        "FLAG 5": 2,
        "FLAG 6": 3,
        "FLAG 4": 3,
        "FLAG 2": 4,
        "FLAG 3": 5,
    }

    def _severity(flag_str):
        for k, v in severity_map.items():
            if flag_str.startswith(k):
                return v
        return 0

    df["anomaly_severity"] = df["anomaly_flag"].apply(_severity)

    total      = len(df)
    flagged    = (df["anomaly_flag"] != "Normal").sum()
    high_risk  = (df["anomaly_severity"] >= 3).sum()

    logger.info(
        f"Anomaly scan complete: {flagged}/{total} flagged "
        f"({flagged/total*100:.1f}%) — {high_risk} high-risk."
    )
    return df


# ─────────────────────────────────────────────────────────────
# 6. Summary report
# ─────────────────────────────────────────────────────────────

def anomaly_summary_report(df: pd.DataFrame) -> dict:
    """Return a structured summary of anomaly flags."""
    flagged = df[df["anomaly_flag"] != "Normal"]

    flag_counts = flagged["anomaly_flag"].str[:6].value_counts().to_dict()

    top_flagged = (
        flagged.sort_values("contract_value", ascending=False)
        [["ocid", "province", "category", "supplier_name",
        "contract_value", "anomaly_flag", "anomaly_severity"]]
        .head(20)
        .to_dict(orient="records")
    )

    by_province = (
        flagged.groupby("province")
        .agg(flag_count=("anomaly_flag", "count"),
            total_flagged_value=("contract_value", "sum"))
        .sort_values("flag_count", ascending=False)
        .to_dict(orient="index")
    )

    return {
        "total_records":         len(df),
        "total_flagged":         len(flagged),
        "flag_rate_pct":         round(len(flagged) / len(df) * 100, 2),
        "high_severity_count":   int((df["anomaly_severity"] >= 3).sum()),
        "flag_counts":           flag_counts,
        "top_flagged_contracts": top_flagged,
        "by_province":           by_province,
    }


