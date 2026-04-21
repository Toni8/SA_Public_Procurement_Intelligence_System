"""
============================================================
 api/main.py
 FastAPI — Procurement Intelligence REST API
 
 Endpoints:
   GET  /health
   POST /predict/value          — contract value forecast
   GET  /opportunities           — top opportunity matrix
   GET  /opportunities/heatmap   — province × quarter pivot
   GET  /anomalies               — flagged contracts
   GET  /anomalies/summary       — aggregated stats
   GET  /recommendations/supplier
   GET  /recommendations/official
   GET  /report/policy-brief
   GET  /benchmarks              — sector × province value benchmarks

 Run:  uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
============================================================
"""

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import API_TITLE, API_VERSION, SA_PROVINCES
from models.value_forecaster    import predict_contract_value, load_model
from models.opportunity_matrix  import (
    build_opportunity_matrix, get_top_opportunities,
    build_heatmap_pivot, load_matrix,
)
from models.anomaly_detector    import (
    run_full_anomaly_pipeline, anomaly_summary_report,
)
from models.recommendation_engine import (
    generate_supplier_recommendations,
    generate_official_recommendations,
    generate_policy_brief,
)
from utils.db_loader import load_master_df

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description="South Africa Public Procurement Intelligence System — eTenders OCDS Data",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory cache (populated at startup) ────────────────────
_cache: dict = {}


@app.on_event("startup")
async def startup_event():
    """Load data and pre-compute matrices at server startup."""
    logger.info("Loading procurement data...")
    try:
        df = load_master_df()
        df = run_full_anomaly_pipeline(df)
        matrix = build_opportunity_matrix(df)
        summary = anomaly_summary_report(df)
        _cache["df"]      = df
        _cache["matrix"]  = matrix
        _cache["summary"] = summary
        logger.info(f"Startup complete — {len(df):,} contracts loaded.")
    except Exception as e:
        logger.warning(f"Startup DB load failed: {e}. Endpoints will require manual load.")


# ─────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────

class PredictRequest(BaseModel):
    province:              str   = Field(..., example="KwaZulu-Natal")
    category:              str   = Field(..., example="Works")
    method:                str   = Field("open", example="open")
    quarter:               int   = Field(..., ge=1, le=4, example=2)
    month:                 int   = Field(..., ge=1, le=12, example=5)
    year:                  int   = Field(2025, example=2025)
    duration_days:         int   = Field(730, ge=1, example=730)
    supplier_prior_awards: int   = Field(0, ge=0, example=0)
    model_name:            str   = Field("ensemble", example="ensemble")


class SupplierRecommendRequest(BaseModel):
    sector:             str            = Field(..., example="Works")
    available_capital:  Optional[float] = Field(None, example=5_000_000)
    preferred_province: Optional[str]  = Field(None, example="KwaZulu-Natal")
    current_quarter:    Optional[int]  = Field(None, ge=1, le=4, example=2)
    top_n:              int            = Field(5, ge=1, le=20, example=5)


# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    return {
        "status": "ok",
        "loaded": "df" in _cache,
        "records": len(_cache.get("df", [])),
    }


@app.post("/predict/value", tags=["Forecasting"])
def predict_value(req: PredictRequest):
    """
    Predict the expected contract value for a given tender profile.
    Returns point estimate + 80% confidence band.
    """
    try:
        result = predict_contract_value(
            province              = req.province,
            category              = req.category,
            method                = req.method,
            quarter               = req.quarter,
            month                 = req.month,
            year                  = req.year,
            duration_days         = req.duration_days,
            supplier_prior_awards = req.supplier_prior_awards,
            model_name            = req.model_name,
        )
        return result
    except FileNotFoundError:
        raise HTTPException(404, "Model not trained yet. Run training pipeline first.")
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/opportunities", tags=["Opportunity Matrix"])
def opportunities(
    sector:           Optional[str] = Query(None, example="Works"),
    province:         Optional[str] = Query(None, example="KwaZulu-Natal"),
    quarter:          Optional[int] = Query(None, ge=1, le=4, example=2),
    top_n:            int           = Query(10, ge=1, le=50),
    min_contracts:    int           = Query(5, ge=1),
    exclude_gauteng:  bool          = Query(False),
):
    """Return ranked opportunity matrix cells matching filters."""
    if "matrix" not in _cache:
        raise HTTPException(503, "Data not loaded. Check /health.")
    result = get_top_opportunities(
        _cache["matrix"], sector, province, quarter,
        top_n, min_contracts, exclude_gauteng,
    )
    return result.to_dict(orient="records")


@app.get("/opportunities/heatmap", tags=["Opportunity Matrix"])
def opportunities_heatmap(
    value_col: str           = Query("median_value", example="opportunity_score"),
    sector:    Optional[str] = Query(None, example="Works"),
):
    """Province × Quarter pivot table for heat-map visualisation."""
    if "matrix" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    pivot = build_heatmap_pivot(_cache["matrix"], value_col, sector)
    return pivot.reset_index().to_dict(orient="records")


@app.get("/anomalies", tags=["Anomaly Detection"])
def anomalies(
    flag:          Optional[str] = Query(None, example="FLAG 3"),
    province:      Optional[str] = Query(None, example="Gauteng"),
    min_severity:  int           = Query(1, ge=0, le=5),
    limit:         int           = Query(50, ge=1, le=500),
):
    """Return flagged contracts, optionally filtered."""
    if "df" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    df = _cache["df"]
    flagged = df[df["anomaly_flag"] != "Normal"].copy()

    if flag:
        flagged = flagged[flagged["anomaly_flag"].str.contains(flag, na=False)]
    if province:
        flagged = flagged[flagged["province"].str.lower() == province.lower()]
    flagged = flagged[flagged["anomaly_severity"] >= min_severity]
    flagged = flagged.sort_values("anomaly_severity", ascending=False).head(limit)

    cols = ["ocid", "province", "category", "method", "supplier_name",
            "contract_value", "tender_date", "anomaly_flag", "anomaly_severity"]
    available = [c for c in cols if c in flagged.columns]
    return flagged[available].to_dict(orient="records")


@app.get("/anomalies/summary", tags=["Anomaly Detection"])
def anomalies_summary():
    """Aggregated anomaly statistics."""
    if "summary" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    return _cache["summary"]


@app.post("/recommendations/supplier", tags=["Recommendations"])
def supplier_recommendations(req: SupplierRecommendRequest):
    """
    Strategic bid recommendations for a supplier.
    Provide your sector, capital capacity, and optional province preference.
    """
    if "matrix" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    return generate_supplier_recommendations(
        matrix             = _cache["matrix"],
        sector             = req.sector,
        available_capital  = req.available_capital,
        preferred_province = req.preferred_province,
        current_quarter    = req.current_quarter,
        top_n              = req.top_n,
    )


@app.get("/recommendations/official", tags=["Recommendations"])
def official_recommendations():
    """Procurement governance recommendations for officials."""
    if "df" not in _cache or "summary" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    return generate_official_recommendations(
        df_flagged     = _cache["df"],
        anomaly_summary = _cache["summary"],
    )


@app.get("/report/policy-brief", tags=["Reports"])
def policy_brief():
    """High-level intelligence brief for policy analysts."""
    if "df" not in _cache or "matrix" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    return generate_policy_brief(
        df             = _cache["df"],
        matrix         = _cache["matrix"],
        anomaly_summary = _cache["summary"],
    )


@app.get("/benchmarks", tags=["Benchmarks"])
def benchmarks(
    sector:   Optional[str] = Query(None, example="Works"),
    province: Optional[str] = Query(None, example="KwaZulu-Natal"),
    min_size: int            = Query(5, ge=1),
):
    """
    Value benchmarks by sector × province.
    Mirrors SQL Question 6 in the BI layer.
    """
    if "df" not in _cache:
        raise HTTPException(503, "Data not loaded.")
    df = _cache["df"]

    if sector:
        df = df[df["category"].str.lower() == sector.lower()]
    if province:
        df = df[df["province"].str.lower() == province.lower()]

    grp = (
        df[df["contract_value"] > 0]
        .groupby(["category", "province"])
        .agg(
            sample_size = ("contract_value", "count"),
            mean_zar    = ("contract_value", "mean"),
            median_zar  = ("contract_value", "median"),
            min_zar     = ("contract_value", "min"),
            max_zar     = ("contract_value", "max"),
            std_zar     = ("contract_value", "std"),
        )
        .reset_index()
    )
    grp = grp[grp["sample_size"] >= min_size]

    def _reliability(n):
        if n >= 10: return "High"
        if n >= 5:  return "Medium"
        return "Low"

    grp["reliability"] = grp["sample_size"].apply(_reliability)
    grp = grp.sort_values("median_zar", ascending=False)
    return grp.to_dict(orient="records")


