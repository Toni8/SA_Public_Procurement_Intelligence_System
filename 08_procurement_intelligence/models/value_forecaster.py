""" ML training and contract value forecasting
============================================================
models/value_forecaster.py
Contract Value Forecasting — Multi-model pipeline

Models trained (log-transformed target):
1. Random Forest Regressor       (baseline tree ensemble)
2. XGBoost Regressor             (gradient boosting)
3. LightGBM Regressor            (fast gradient boosting)
4. Ridge Regression              (linear baseline)
5. VotingRegressor (soft ensemble of top 3)

EDA key insight implemented:
- interaction terms: province × quarter × sector
- log-transform of target
- median-anchored reporting
============================================================
"""

import numpy as np
import pandas as pd
import joblib
import logging
from pathlib import Path
import sys

from sklearn.ensemble import RandomForestRegressor, VotingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    logging.warning("xgboost not installed — skipping XGB model.")

try:
    from lightgbm import LGBMRegressor
    HAS_LGB = True
except ImportError:
    HAS_LGB = False
    logging.warning("lightgbm not installed — skipping LGB model.")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import RANDOM_STATE, TEST_SIZE, CV_FOLDS, MODEL_DIR
from utils.feature_engineering import build_feature_matrix, decode_prediction, format_zar

logger = logging.getLogger(__name__)

MODEL_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# 1. Model definitions
# ─────────────────────────────────────────────────────────────

def _build_models() -> dict:
    models = {}

    models["ridge"] = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  Ridge(alpha=10.0)),
    ])

    models["random_forest"] = RandomForestRegressor(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=RANDOM_STATE,
    )

    if HAS_XGB:
        models["xgboost"] = XGBRegressor(
            n_estimators=400,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=RANDOM_STATE,
            verbosity=0,
        )

    if HAS_LGB:
        models["lightgbm"] = LGBMRegressor(
            n_estimators=400,
            learning_rate=0.05,
            num_leaves=63,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=RANDOM_STATE,
            verbose=-1,
        )

    # Build soft ensemble from available boosting models
    ensemble_estimators = []
    if HAS_XGB:
        ensemble_estimators.append(("xgb", models["xgboost"]))
    if HAS_LGB:
        ensemble_estimators.append(("lgb", models["lightgbm"]))
    ensemble_estimators.append(("rf", models["random_forest"]))

    if len(ensemble_estimators) >= 2:
        models["ensemble"] = VotingRegressor(estimators=ensemble_estimators)

    return models


# ─────────────────────────────────────────────────────────────
# 2. Training runner
# ─────────────────────────────────────────────────────────────

def train_all_models(df: pd.DataFrame) -> dict:
    """
    Train all models on df.
    Returns dict: {model_name: {model, metrics, feature_names}}
    Also saves each model to MODEL_DIR.
    """
    X, y, label_enc, freq_enc = build_feature_matrix(df)
    feature_names = list(X.columns)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    logger.info(f"Train: {len(X_train):,}  |  Test: {len(X_test):,}")

    models  = _build_models()
    results = {}
    kf      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)

    for name, model in models.items():
        logger.info(f"Training {name}...")
        model.fit(X_train, y_train)

        y_pred_log = model.predict(X_test)
        y_pred_zar = np.expm1(y_pred_log)
        y_true_zar = np.expm1(y_test)

        cv_scores  = cross_val_score(model, X_train, y_train,
                                    cv=kf, scoring="r2", n_jobs=-1)

        mae_log    = mean_absolute_error(y_test, y_pred_log)
        r2_log     = r2_score(y_test, y_pred_log)
        mape       = np.mean(np.abs((y_true_zar - y_pred_zar) /
                                    (y_true_zar + 1))) * 100

        metrics = {
            "mae_log":         round(mae_log,  4),
            "r2_log":          round(r2_log,   4),
            "mape_pct":        round(mape,     2),
            "cv_r2_mean":      round(cv_scores.mean(), 4),
            "cv_r2_std":       round(cv_scores.std(),  4),
        }
        logger.info(f"  {name}: R²={r2_log:.4f}  CV-R²={cv_scores.mean():.4f}±{cv_scores.std():.4f}  MAPE={mape:.1f}%")

        # Save model
        out_path = MODEL_DIR / f"{name}.pkl"
        joblib.dump({"model": model, "feature_names": feature_names,
                    "label_enc": label_enc, "freq_enc": freq_enc}, out_path)
        logger.info(f"  Saved -> {out_path}")

        results[name] = {
            "model":         model,
            "metrics":       metrics,
            "feature_names": feature_names,
        }

    # ── Feature importance (best tree model) ──────────────
    best_tree = None
    for nm in ["lightgbm", "xgboost", "random_forest"]:
        if nm in results:
            best_tree = (nm, results[nm]["model"])
            break

    if best_tree:
        nm, m = best_tree
        if hasattr(m, "feature_importances_"):
            fi = pd.Series(m.feature_importances_, index=feature_names)
        else:
            fi = pd.Series(
                m.named_steps["model"].coef_ if hasattr(m, "named_steps") else [],
                index=feature_names[:len(m.named_steps["model"].coef_)]
                if hasattr(m, "named_steps") else feature_names
            )
        fi_path = MODEL_DIR / "feature_importance.csv"
        fi.sort_values(ascending=False).to_csv(fi_path, header=["importance"])
        logger.info(f"Feature importance saved -> {fi_path}")

    return results


# ─────────────────────────────────────────────────────────────
# 3. Inference helper
# ─────────────────────────────────────────────────────────────

def load_model(model_name: str = "ensemble"):
    """Load a saved model bundle from MODEL_DIR."""
    path = MODEL_DIR / f"{model_name}.pkl"
    if not path.exists():
        raise FileNotFoundError(f"Model not found: {path}")
    return joblib.load(path)


def predict_contract_value(
    province: str,
    category: str,
    method: str,
    quarter: int,
    month: int,
    year: int,
    duration_days: int = 730,
    supplier_prior_awards: int = 0,
    model_name: str = "ensemble",
) -> dict:
    """
    Predict contract value for a given tender profile.
    Returns dict with ZAR prediction + confidence band.
    """
    bundle = load_model(model_name)
    model  = bundle["model"]
    fnames = bundle["feature_names"]
    lenc   = bundle["label_enc"]
    fenc   = bundle["freq_enc"]

    # Build raw row
    row = {
        "province":              province,
        "category":              category,
        "method":                method,
        "duration_band":         _days_to_band(duration_days),
        "month":                 month,
        "quarter":               quarter,
        "year":                  year,
        "duration_days":         duration_days,
        "supplier_prior_awards": supplier_prior_awards,
        "is_new_supplier":       int(supplier_prior_awards == 0),
        "is_year_end_quarter":   int(quarter == 4),
        "is_peak_month":         int(month == 8),
        "province_quarter":      f"{province}_Q{quarter}",
        "prov_cat_q":            f"{province}_{category}_Q{quarter}",
    }

    df_row = pd.DataFrame([row])

    # Apply encoders
    df_row[["province", "category", "method"]] = lenc.transform(
        df_row[["province", "category", "method"]]
    )
    duration_order = {"<3mo": 0, "3-6mo": 1, "6-12mo": 2,
                    "1-2yr": 3, "2-3yr": 4, ">3yr": 5}
    df_row["duration_band"] = df_row["duration_band"].map(duration_order).fillna(-1)
    df_row[["province_quarter", "prov_cat_q"]] = fenc.transform(
        df_row[["province_quarter", "prov_cat_q"]]
    )
    df_row = df_row[fnames].fillna(0)

    log_pred  = float(model.predict(df_row)[0])
    zar_pred  = decode_prediction(log_pred)

    # Approximate 80% confidence band via log ± 1 MAE
    MAE_LOG   = 0.85   # replace with actual MAE from training results
    zar_low   = decode_prediction(log_pred - MAE_LOG)
    zar_high  = decode_prediction(log_pred + MAE_LOG)

    return {
        "predicted_value_zar":  round(zar_pred,  2),
        "formatted":            format_zar(zar_pred),
        "confidence_low_zar":   round(zar_low,   2),
        "confidence_high_zar":  round(zar_high,  2),
        "confidence_low_fmt":   format_zar(zar_low),
        "confidence_high_fmt":  format_zar(zar_high),
        "log_prediction":       round(log_pred,  4),
        "model_used":           model_name,
    }


def _days_to_band(days: int) -> str:
    if days <= 90:   return "<3mo"
    if days <= 180:  return "3-6mo"
    if days <= 365:  return "6-12mo"
    if days <= 730:  return "1-2yr"
    if days <= 1095: return "2-3yr"
    return ">3yr"


# ─────────────────────────────────────────────────────────────
# 4. Model comparison report
# ─────────────────────────────────────────────────────────────

def print_model_comparison(results: dict):
    rows = []
    for name, res in results.items():
        m = res["metrics"]
        rows.append({
            "Model":          name,
            "R² (test)":      m["r2_log"],
            "CV R² (mean)":   m["cv_r2_mean"],
            "CV R² (std)":    m["cv_r2_std"],
            "MAE (log)":      m["mae_log"],
            "MAPE %":         m["mape_pct"],
        })
    df = pd.DataFrame(rows).sort_values("CV R² (mean)", ascending=False)
    print("\n" + "="*70)
    print("  MODEL COMPARISON — Contract Value Forecasting")
    print("="*70)
    print(df.to_string(index=False))
    print("="*70 + "\n")
    return df