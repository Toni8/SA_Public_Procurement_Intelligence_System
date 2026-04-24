# 🏛 South Africa Public Procurement Intelligence System

**End‑to‑end machine learning & business intelligence platform for eTender data (OCDS format)**  
*Powered by MySQL · Streamlit · FastAPI · Scikit‑learn*

---

## 📌 Overview

This project transforms raw public procurement data into **actionable intelligence** across **43,000+ tenders** and **8,000+ awards**.

**It enables:**
- **Suppliers** → Identify where and when to bid, and estimate contract values  
- **Procurement officials** → Detect governance risks and anomalies  
- **Policy analysts** → Understand spending patterns and market concentration  

The system combines machine learning, analytics, and automation into a production‑ready pipeline.

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| 📊 **Interactive Dashboard** | Multi‑page Streamlit app with live charts, opportunity matrix, value forecaster, anomaly monitor, and supplier strategy |
| 🌐 **REST API** | 10 FastAPI endpoints for programmatic access to predictions, opportunities, and anomalies |
| 🤖 **ML Forecasting** | Random Forest, XGBoost, LightGBM, Ridge, Voting Ensemble – predicts contract values on the full award dataset |
| 📈 **Full Dataset Training** | Uses all available awards (~5,600 rows) to capture the real distribution – no arbitrary value filtering |
| 🚨 **Anomaly Detection** | 7 governance red flags: restricted methods, mega‑contracts, new‑supplier awards, statistical outliers, concentration risk, quarterly spikes |
| 🧭 **Opportunity Matrix** | Province × Sector × Quarter scoring with reliability tiers and seasonal weights |
| 📄 **Excel Reporting** | Automatically generates a professionally formatted 6‑sheet workbook |
| ⏰ **Automated Scheduler** | Weekly full retrain, daily anomaly refresh, mid‑week matrix rebuild – completely hands‑off |
| 🛢️ **MySQL Integration** | Direct connection to OCDS staging tables with automatic feature engineering |
| 🧪 **Synthetic Data Support** | Run the entire pipeline without a database using realistic synthetic data |

---

## 📊 Architecture (Simplified Flow)

1. **MySQL (OCDS Staging Tables)**
2. **Data Loading & EDA** – Fetch, clean, and explore
3. **Feature Engineering** – Build interaction terms, temporal & duration features, target encoding
4. **Anomaly Detection** + **Opportunity Matrix** – Apply 7 governance flags, score province×sector×quarter cells
5. **ML Training Pipeline** – Train Random Forest, XGBoost, LightGBM, Ensemble models; save to `.pkl`
6. **Trained Models** → **Dashboard (Streamlit)** / **REST API (FastAPI)** / **Excel Reporter (openpyxl)**
7. **Scheduler (APScheduler)** – Automates retraining, anomaly refreshes, matrix updates

---

## 📁 Data

**Primary Source:** MySQL database `procurement_intelligence`  
**Tables:** `main_staging`, `awards_staging`, `contracts_staging`, `awards_suppliers_staging`  
**Fallback:** Synthetic data generator (`utils/generate_sample_data.py`) – 10,000 realistic records  

**Training Strategy:**  
Trains on the **full award dataset** with no value filtering. This allows the model to learn the complete market distribution – from small purchases to billion‑rand mega‑deals – making it more robust in production.

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repo-url>
cd procurement_intelligence
