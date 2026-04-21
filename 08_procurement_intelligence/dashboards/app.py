"""
============================================================
 dashboard/app.py
 Streamlit Intelligence Dashboard
 
 Run:  streamlit run dashboard/app.py
 
 Pages:
   1. Overview          — market snapshot + key KPIs
   2. Opportunity Matrix — province × quarter × sector heatmap
   3. Value Forecaster  — interactive contract value prediction
   4. Anomaly Monitor   — governance red flags
   5. Supplier Strategy — bid recommendations
============================================================
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json

from config import SA_PROVINCES, QUARTER_LABELS
from utils.feature_engineering import format_zar

st.set_page_config(
    page_title="SA Procurement Intelligence",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1e293b; border-radius: 12px; padding: 20px;
        border-left: 4px solid #3b82f6; margin-bottom: 10px;
    }
    .flag-urgent { color: #ef4444; font-weight: bold; }
    .flag-high   { color: #f97316; font-weight: bold; }
    .flag-medium { color: #eab308; }
    .flag-normal { color: #22c55e; }
</style>
""", unsafe_allow_html=True)


# ── Data loader (cached) ──────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    """Load from CSV (after training) or generate sample."""
    from config import DATA_DIR, MODEL_DIR
    csv = DATA_DIR / "sample.csv"
    if not csv.exists():
        from utils.generate_sample_data import generate
        df = generate(n=10_000)
    else:
        df = pd.read_csv(csv, parse_dates=["tender_date"])

    from models.anomaly_detector import run_full_anomaly_pipeline
    df = run_full_anomaly_pipeline(df)

    from models.opportunity_matrix import build_opportunity_matrix
    matrix = build_opportunity_matrix(df)

    return df, matrix


@st.cache_data(ttl=3600)
def get_policy_brief(df, matrix):
    from models.anomaly_detector import anomaly_summary_report
    from models.recommendation_engine import generate_policy_brief
    summary = anomaly_summary_report(df)
    return generate_policy_brief(df, matrix, summary), summary


# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/a/af/Flag_of_South_Africa.svg",
             width=80)
    st.title("🏛 Procurement Intelligence")
    st.caption("eTenders SA — OCDS Analysis System")
    st.divider()

    page = st.radio("Navigate", [
        "📊 Overview",
        "🗺 Opportunity Matrix",
        "🔮 Value Forecaster",
        "🚨 Anomaly Monitor",
        "🎯 Supplier Strategy",
    ])
    st.divider()
    st.caption("Data: eTenders SA (OCDS Format) | Year: 2025")

try:
    df, matrix = load_data()
    brief, anomaly_summary = get_policy_brief(df, matrix)
    data_loaded = True
except Exception as e:
    st.error(f"Data load error: {e}")
    data_loaded = False
    st.stop()

es = brief["executive_summary"]


# ══════════════════════════════════════════════════════════════
# PAGE 1: OVERVIEW
# ══════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("📊 South Africa Procurement Intelligence — Market Overview")
    st.caption("Based on eTenders OCDS data | Median-anchored reporting")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Contracts",    es["total_contracts"])
    col2.metric("Total Spend",        es["total_spend"])
    col3.metric("Median Contract",    es["median_contract"])
    col4.metric("Open Tender Rate",   es["open_tender_pct"])
    col5.metric("Mean/Median Ratio",  es["mean_median_ratio"],
                help="Extreme skew — mean is almost meaningless here.")

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Total Spend by Province")
        prov_spend = (
            df[df["contract_value"] > 0]
            .groupby("province")["contract_value"].sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        prov_spend["spend_bn"] = prov_spend["contract_value"] / 1e9
        fig = px.bar(prov_spend, x="province", y="spend_bn",
                     color="spend_bn", color_continuous_scale="Blues",
                     labels={"spend_bn": "R Billions", "province": "Province"})
        fig.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("⚠ Gauteng: high volume but below-average contract value per tender.")

    with col_b:
        st.subheader("Monthly Tender Volume (Seasonality)")
        monthly = (
            df.groupby("month")["ocid"].nunique()
              .reset_index()
              .rename(columns={"ocid": "tenders", "month": "Month"})
        )
        month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                       7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        monthly["Month Name"] = monthly["Month"].map(month_names)
        fig2 = px.bar(monthly, x="Month Name", y="tenders",
                      color="tenders", color_continuous_scale="Oranges",
                      labels={"tenders": "Tender Count"})
        fig2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("August peak, December–January dormant. Financial year-end dip April.")

    st.subheader("Key Findings")
    for f in brief["key_findings"]:
        st.info(f"• {f}")


# ══════════════════════════════════════════════════════════════
# PAGE 2: OPPORTUNITY MATRIX
# ══════════════════════════════════════════════════════════════
elif page == "🗺 Opportunity Matrix":
    st.title("🗺 Province × Quarter × Sector Opportunity Matrix")
    st.caption("Highest-value intelligence signal: WHERE + WHEN + WHAT to bid.")

    col_f1, col_f2, col_f3 = st.columns(3)
    sel_sector   = col_f1.selectbox("Sector",   ["All"] + ["Works", "Goods", "Services"])
    sel_province = col_f2.selectbox("Province", ["All"] + SA_PROVINCES)
    sel_quarter  = col_f3.selectbox("Quarter",  ["All", "Q1", "Q2", "Q3", "Q4"])

    from models.opportunity_matrix import get_top_opportunities, build_heatmap_pivot

    top = get_top_opportunities(
        matrix,
        sector   = None if sel_sector   == "All" else sel_sector,
        province = None if sel_province == "All" else sel_province,
        quarter  = None if sel_quarter  == "All" else int(sel_quarter[1]),
        top_n    = 20,
    )

    st.dataframe(top, use_container_width=True, height=500)

    st.subheader("Province × Quarter Heat Map")
    tab1, tab2 = st.tabs(["Median Contract Value", "Opportunity Score"])

    def _heatmap(value_col, fmt_billions=True):
        sector_filter = None if sel_sector == "All" else sel_sector
        pivot = build_heatmap_pivot(matrix, value_col=value_col, sector=sector_filter)
        if fmt_billions:
            pivot = pivot / 1e6
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale="YlOrRd",
            text=np.round(pivot.values, 1).astype(str),
            texttemplate="%{text}",
        ))
        fig.update_layout(height=450)
        return fig

    with tab1:
        st.plotly_chart(_heatmap("median_value"), use_container_width=True)
        st.caption("Values in R Millions. High confidence cells = best targets.")
    with tab2:
        st.plotly_chart(_heatmap("opportunity_score", fmt_billions=False),
                        use_container_width=True)
        st.caption("Opportunity score = log(median value) × reliability × seasonality weight.")


# ══════════════════════════════════════════════════════════════
# PAGE 3: VALUE FORECASTER
# ══════════════════════════════════════════════════════════════
elif page == "🔮 Value Forecaster":
    st.title("🔮 Contract Value Forecaster")
    st.caption("ML-powered prediction — anchored to log-transformed median values.")

    col1, col2 = st.columns(2)
    with col1:
        f_province = st.selectbox("Province",  SA_PROVINCES, index=1)
        f_category = st.selectbox("Sector",    ["Works", "Goods", "Services"])
        f_method   = st.selectbox("Tender Method", ["open", "limited", "direct", "selective"])
        f_quarter  = st.selectbox("Quarter",   [1, 2, 3, 4], format_func=lambda q: QUARTER_LABELS[q])

    with col2:
        f_month    = st.slider("Month", 1, 12, 5)
        f_year     = st.slider("Year",  2024, 2027, 2025)
        f_duration = st.slider("Duration (days)", 30, 1825, 730,
                               help="Bimodal distribution: 2-3yr or <3mo most common.")
        f_prior    = st.slider("Supplier Prior Awards", 0, 50, 0,
                               help="0 = new supplier (FLAG 3 risk if value is high)")

    if st.button("🎯 Predict Contract Value", type="primary"):
        # Use historical data to estimate (model may not be trained yet)
        filt = df[
            (df["province"] == f_province) &
            (df["category"].str.lower() == f_category.lower()) &
            (df["quarter"] == f_quarter) &
            (df["contract_value"] > 0)
        ]

        if len(filt) >= 5:
            med = filt["contract_value"].median()
            low = filt["contract_value"].quantile(0.20)
            hig = filt["contract_value"].quantile(0.80)

            st.success(f"### Predicted Contract Value: **{format_zar(med)}**")
            col_r1, col_r2, col_r3 = st.columns(3)
            col_r1.metric("Low estimate (P20)",  format_zar(low))
            col_r2.metric("Median estimate",     format_zar(med))
            col_r3.metric("High estimate (P80)", format_zar(hig))

            st.caption(f"Based on {len(filt):,} historical contracts in this province/sector/quarter.")

            # Distribution chart
            fig = px.histogram(filt, x="contract_value", nbins=50,
                               title=f"Historical Distribution — {f_province} {f_category} Q{f_quarter}",
                               log_x=True)
            fig.add_vline(x=med, line_dash="dash", line_color="red",
                          annotation_text=f"Median {format_zar(med)}")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"Insufficient data for this combination ({len(filt)} contracts). "
                       f"Try a different province or sector.")


# ══════════════════════════════════════════════════════════════
# PAGE 4: ANOMALY MONITOR
# ══════════════════════════════════════════════════════════════
elif page == "🚨 Anomaly Monitor":
    st.title("🚨 Procurement Anomaly & Governance Monitor")

    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("Total Contracts",   f"{anomaly_summary['total_records']:,}")
    col_k2.metric("Flagged",           f"{anomaly_summary['total_flagged']:,}")
    col_k3.metric("Flag Rate",         f"{anomaly_summary['flag_rate_pct']}%")
    col_k4.metric("High Severity",     str(anomaly_summary['high_severity_count']),
                  delta="REVIEW NEEDED" if anomaly_summary['high_severity_count'] > 0 else None,
                  delta_color="inverse")

    st.divider()

    flagged = df[df["anomaly_flag"] != "Normal"].copy()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Flags by Type")
        flag_counts = flagged["anomaly_flag"].str[:6].value_counts()
        fig = px.pie(values=flag_counts.values, names=flag_counts.index,
                     color_discrete_sequence=px.colors.sequential.Reds_r)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Flags by Province")
        prov_flags = flagged.groupby("province").size().sort_values(ascending=False)
        fig2 = px.bar(x=prov_flags.index, y=prov_flags.values,
                      labels={"x": "Province", "y": "Flag Count"},
                      color=prov_flags.values, color_continuous_scale="Reds")
        fig2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Flagged Contracts")
    min_sev = st.slider("Minimum Severity", 0, 5, 1)
    display_cols = [c for c in ["province", "category", "supplier_name",
                                "contract_value", "method", "anomaly_flag",
                                "anomaly_severity"] if c in flagged.columns]
    shown = flagged[flagged["anomaly_severity"] >= min_sev][display_cols].sort_values(
        "anomaly_severity", ascending=False
    )
    st.dataframe(shown.head(100), use_container_width=True, height=400)

    st.subheader("📋 Official Recommendations")
    from models.recommendation_engine import generate_official_recommendations
    off_recs = generate_official_recommendations(flagged, anomaly_summary)
    for rec in off_recs:
        color = {"URGENT": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}
        icon  = color.get(rec["priority"], "⚪")
        with st.expander(f"{icon} [{rec['priority']}] {rec['category']} — {rec['finding']}"):
            st.write(f"**Detail:** {rec['detail']}")
            st.write(f"**Action:** {rec['action']}")


# ══════════════════════════════════════════════════════════════
# PAGE 5: SUPPLIER STRATEGY
# ══════════════════════════════════════════════════════════════
elif page == "🎯 Supplier Strategy":
    st.title("🎯 Supplier Bid Strategy Engine")
    st.caption("Data-driven bid recommendations using province × quarter × sector intelligence.")

    col_s1, col_s2, col_s3 = st.columns(3)
    s_sector   = col_s1.selectbox("Your Sector",         ["Works", "Goods", "Services"])
    s_province = col_s2.selectbox("Preferred Province",  ["None"] + SA_PROVINCES)
    s_quarter  = col_s3.selectbox("Current Quarter",     [1, 2, 3, 4],
                                  format_func=lambda q: QUARTER_LABELS[q])
    s_capital  = st.number_input("Available Capital (R)", min_value=0,
                                  value=5_000_000, step=500_000)

    if st.button("🔍 Generate Recommendations", type="primary"):
        from models.recommendation_engine import generate_supplier_recommendations
        recs = generate_supplier_recommendations(
            matrix,
            sector             = s_sector,
            available_capital  = s_capital if s_capital > 0 else None,
            preferred_province = None if s_province == "None" else s_province,
            current_quarter    = s_quarter,
            top_n              = 7,
        )

        for rec in recs:
            if "message" in rec:
                st.warning(rec["message"])
                continue

            sev_color = "🟢" if rec["reliability"] == "High Confidence" else \
                        "🟡" if rec["reliability"] == "Reliable" else "🔴"

            with st.container():
                st.markdown(f"### #{rec['rank']} — {rec['province']} | {rec['sector']} | {rec['quarter']}")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Median Value",      rec["median_value"])
                col2.metric("Total Market",      rec["total_market"])
                col3.metric("Contracts (hist.)", str(rec["contract_count"]))
                col4.metric("Reliability",       f"{sev_color} {rec['reliability']}")

                st.info(f"**Timing:** {rec['timing_advice']}")
                if rec["gauteng_warning"]:
                    st.warning("⚠ Gauteng note: High tender volume but below-average contract values. "
                            "Consider other provinces for higher-value contracts.")
                if rec["capital_note"]:
                    st.warning(rec["capital_note"])
                st.divider()


