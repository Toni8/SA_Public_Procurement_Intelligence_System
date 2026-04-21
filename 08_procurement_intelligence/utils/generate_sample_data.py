"""
Synthetic data generator – mirrors real eTenders schema.
"""

import numpy as np
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import SA_PROVINCES, DATA_DIR

np.random.seed(42)
DATA_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES = ["Works", "Goods", "Services"]
METHODS = ["open"] * 85 + ["direct"] * 7 + ["limited"] * 5 + ["selective"] * 3
BUYERS = ["Dept of Public Works", "Dept of Health", "Dept of Education",
        "Eskom", "SANRAL", "City of Johannesburg", "eThekwini", "City of Cape Town"]
SUPPLIERS = [f"Supplier_{i:04d}" for i in range(1, 501)]

def _contract_value(province, category, quarter):
    base = {"Works": 8_000_000, "Goods": 2_500_000, "Services": 1_800_000}[category]
    prov_mult = {"KwaZulu-Natal": 2.1, "Eastern Cape": 1.7, "Western Cape": 1.5,
                "Gauteng": 0.6, "Limpopo": 1.2, "Mpumalanga": 1.1,
"North West": 0.9, "Free State": 0.8, "Northern Cape": 0.7}.get(province, 1.0)
    q_mult = {1: 0.85, 2: 1.10, 3: 1.25, 4: 0.95}[quarter]
    median_val = base * prov_mult * q_mult
    val = np.random.lognormal(np.log(median_val), 1.4)
    if np.random.random() < 0.08:
        val = np.random.uniform(1000, 50000)
    return round(val, 2)

def generate(n=10000):
    rows = []
    for i in range(n):
        province = np.random.choice(SA_PROVINCES, p=[0.28,0.18,0.13,0.10,0.08,0.07,0.06,0.06,0.04])
        category = np.random.choice(CATEGORIES, p=[0.40,0.30,0.30])
        method = np.random.choice(METHODS)
        quarter = np.random.choice([1,2,3,4], p=[0.22,0.25,0.30,0.23])
        month = np.random.choice({1:[1,2,3],2:[4,5,6],3:[7,8,9],4:[10,11,12]}[quarter])
        year = np.random.choice([2022,2023,2024,2025], p=[0.15,0.25,0.35,0.25])
        dur_type = np.random.choice(["short","medium","long"], p=[0.20,0.55,0.25])
        dur_days = {"short": np.random.randint(10,90),
                    "medium": np.random.randint(540,1095),
                    "long": np.random.randint(1096,1825)}[dur_type]
        supplier = SUPPLIERS[np.random.choice(len(SUPPLIERS))]
        if np.random.random() < 0.4:
            supplier = SUPPLIERS[int(np.random.exponential(30)) % 80]
        val = _contract_value(province, category, quarter)
        rows.append({
            "ocid": f"ocds-{i:08d}",
            "buyer_name": np.random.choice(BUYERS),
            "province": province,
            "category": category,
            "method": method,
            "tender_date": pd.Timestamp(year=year, month=month, day=np.random.randint(1,28)),
            "contract_id": f"c-{i:08d}",
            "contract_value": val,
            "duration_days": dur_days,
            "award_id": f"a-{i:08d}",
            "award_value": val * np.random.uniform(0.95, 1.05),
            "supplier_name": supplier,
            "year": year,
            "month": month,
            "quarter": quarter,
        })
    df = pd.DataFrame(rows)
    df["log_value"] = np.log1p(df["contract_value"])
    df["duration_years"] = df["duration_days"] / 365.25
    df["is_year_end_quarter"] = (df["quarter"] == 4).astype(int)
    df["is_peak_month"] = (df["month"] == 8).astype(int)
    df["province_quarter"] = df["province"] + "_Q" + df["quarter"].astype(str)
    df["prov_cat_q"] = df["province"] + "_" + df["category"] + "_Q" + df["quarter"].astype(str)
    sup_counts = df.groupby("supplier_name")["award_id"].count().rename("supplier_prior_awards")
    df = df.merge(sup_counts, on="supplier_name", how="left")
    df["is_new_supplier"] = (df["supplier_prior_awards"] == 1).astype(int)
    df["duration_band"] = pd.cut(df["duration_days"],
                                bins=[0,90,180,365,730,1095,np.inf],
                                labels=["<3mo","3-6mo","6-12mo","1-2yr","2-3yr",">3yr"])
    out = DATA_DIR / "sample.csv"
    df.to_csv(out, index=False)
    print(f"✅ Sample data generated: {len(df):,} rows -> {out}")
    return df

if __name__ == "__main__":
    generate()