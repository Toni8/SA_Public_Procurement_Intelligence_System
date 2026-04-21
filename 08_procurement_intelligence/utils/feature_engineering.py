""" Feature engineering pipeline """
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelEncoder
import logging

logger = logging.getLogger(__name__)

CAT_COLS = ["province", "category", "method", "duration_band"]
NUM_COLS = ["month", "quarter", "year", "duration_days", "supplier_prior_awards",
            "is_new_supplier", "is_year_end_quarter", "is_peak_month"]
INTERACTION_COLS = ["province_quarter", "prov_cat_q"]

class SafeLabelEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, cols=None):
        self.cols = cols
        self.encoders_ = {}
    def fit(self, X, y=None):
        df = pd.DataFrame(X) if not isinstance(X, pd.DataFrame) else X.copy()
        cols = self.cols or df.select_dtypes(include="object").columns
        for c in cols:
            le = LabelEncoder()
            le.fit(df[c].astype(str).fillna("__NA__"))
            self.encoders_[c] = le
        return self
    def transform(self, X):
        df = pd.DataFrame(X) if not isinstance(X, pd.DataFrame) else X.copy()
        for c, le in self.encoders_.items():
            known = set(le.classes_)
            df[c] = df[c].astype(str).fillna("__NA__").map(lambda v: v if v in known else "__UNSEEN__")
            if "__UNSEEN__" not in le.classes_:
                le.classes_ = np.append(le.classes_, "__UNSEEN__")
            df[c] = le.transform(df[c])
        return df

class FrequencyEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, cols=None):
        self.cols = cols
        self.freq_maps_ = {}
    def fit(self, X, y=None):
        df = pd.DataFrame(X) if not isinstance(X, pd.DataFrame) else X.copy()
        cols = self.cols or df.select_dtypes(include="object").columns
        for c in cols:
            self.freq_maps_[c] = df[c].value_counts(normalize=True).to_dict()
        return self
    def transform(self, X):
        df = pd.DataFrame(X) if not isinstance(X, pd.DataFrame) else X.copy()
        for c, fm in self.freq_maps_.items():
            df[c] = df[c].map(fm).fillna(0.0)
        return df

def build_feature_matrix(df: pd.DataFrame):
    required = CAT_COLS + NUM_COLS + INTERACTION_COLS + ["log_value"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    feat_df = df[CAT_COLS + NUM_COLS + INTERACTION_COLS].copy()

    # Convert duration_band from categorical to integer safely
    duration_order = {"<3mo": 0, "3-6mo": 1, "6-12mo": 2, "1-2yr": 3, "2-3yr": 4, ">3yr": 5}
    if hasattr(feat_df["duration_band"], "cat"):
        # If it's categorical, convert to string first
        feat_df["duration_band"] = feat_df["duration_band"].astype(str)
    feat_df["duration_band"] = feat_df["duration_band"].map(duration_order).fillna(-1).astype(int)

    # Encode base categoricals
    enc = SafeLabelEncoder(cols=["province", "category", "method"])
    feat_df[["province", "category", "method"]] = enc.fit_transform(
        feat_df[["province", "category", "method"]]
    )

    # Frequency-encode interaction strings
    freq_enc = FrequencyEncoder(cols=["province_quarter", "prov_cat_q"])
    feat_df[["province_quarter", "prov_cat_q"]] = freq_enc.fit_transform(
        feat_df[["province_quarter", "prov_cat_q"]]
    )

    feat_df = feat_df.fillna(0)
    y = df["log_value"]

    logger.info(f"Feature matrix: {feat_df.shape}, target range [{y.min():.2f}, {y.max():.2f}]")
    return feat_df, y, enc, freq_enc

def decode_prediction(log_pred: float) -> float:
    return float(np.expm1(log_pred))

def format_zar(value: float) -> str:
    if value >= 1e9: return f"R {value/1e9:.2f}B"
    if value >= 1e6: return f"R {value/1e6:.2f}M"
    if value >= 1e3: return f"R {value/1e3:.1f}K"
    return f"R {value:,.0f}"