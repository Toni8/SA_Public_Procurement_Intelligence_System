# 🏛 South Africa Public Procurement Intelligence System

**End‑to‑end machine learning & business intelligence solution for e‑tender data (OCDS format)**  
*Data source: South African eTenders portal · MySQL staging tables*

---

## 📌 Overview

This system transforms raw procurement data from over **43,000 tenders** and **8,000 awards** into **actionable insights** for:

- **Suppliers** – identify where and when to bid, predict contract values  
- **Procurement officials** – monitor governance red flags  
- **Policy analysts** – understand spending patterns and market concentration  

It provides a **Streamlit dashboard**, a **FastAPI REST API**, an **automated Excel report generator**, and a **background scheduler** for continuous updates.

---

## ✨ Key Features

- **Interactive Dashboard** (Streamlit) – 5‑page web app with live charts, opportunity matrix, value forecaster, anomaly monitor, and supplier strategy.  
- **REST API** (FastAPI) – 10 endpoints for integration with other tools.  
- **Automated Excel Reporter** – produces a 6‑sheet formatted workbook on demand.  
- **ML‑based Contract Value Forecasting** – Random Forest, XGBoost, LightGBM, Voting Ensemble; R² up to **0.40** on filtered data.  
- **7 Governance Anomaly Flags** – restricted methods, new‑supplier mega‑deals, statistical outliers, etc.  
- **Opportunity Matrix** – province × sector × quarter scoring with reliability tiers.  
- **Background Scheduler** – weekly retrain, daily anomaly scan, mid‑week matrix refresh.  
- **MySQL Integration** – direct connection to OCDS staging tables, with automatic feature engineering.  
- **Synthetic Data Generator** – test the full pipeline without a database.

---

## 📊 Architecture Overview
MySQL (staging tables)
│
┌───────────┴───────────┐
│ Data Loader / EDA │
└───────────┬───────────┘
│
┌─────────────────┼─────────────────┐
│ │ │
Anomaly Detector Opportunity Matrix Feature Engineering
│ │ │
└─────────────────┼──────────────────┘
│
ML Training Pipeline
│
┌─────────────────┼──────────────────┐
│ │ │
Models (pkl) API (FastAPI) Dashboard (Streamlit)
│
Auto Scheduler / Excel Reporter

text

---

## 📁 Data Sources

**Primary source:** MySQL database `procurement_intelligence` with standard OCDS staging tables (`main_staging`, `contracts_staging`, `awards_staging`, etc.).  
**Fallback / testing:** Synthetic data generator (`utils/generate_sample_data.py`) creates 10,000 realistic records.

During development, a **smart filtering step** was applied: only awards that had a completed contract **or** an award value above R500,000 were kept for model training (≈1,800 rows). The full dataset (5,679 awards) is used for anomaly detection and matrix building.

---

## 🚀 Quick Start

### 1️⃣ Clone the repository & navigate to the project

```bash
git clone <repo-url>
cd 08_procurement_intelligence
2️⃣ Create and activate a Python environment
bash
conda create -n procurement python=3.11
conda activate procurement
3️⃣ Install dependencies
bash
pip install -r requirements.txt
4️⃣ Configure the database connection
Edit config.py with your MySQL credentials:

python
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "procurement_intelligence",
    "charset": "utf8mb4",
}
If you do not have a MySQL database, you can skip this and use the synthetic data generator (see Section 6).

🧪 5. Run the Training Pipeline
The master script train_pipeline.py loads data, detects anomalies, builds the opportunity matrix, trains all models, and saves reports.

With MySQL (your real data):

bash
python train_pipeline.py
Without MySQL (synthetic data):

bash
# Generate sample data first (one‑time)
python utils/generate_sample_data.py

# Then run the pipeline
python train_pipeline.py --skip-db
After completion, you’ll find:

Trained models in models/

CSV/JSON reports in reports/

The master dataset with anomalies at data/master_with_anomalies.csv

📓 6. Interactive Analysis (Jupyter Notebook)
For step‑by‑step exploration, open notebooks/ml.ipynb. It contains cells to:

Connect to MySQL (or load the synthetic CSV)

Apply feature engineering and smart filtering

Detect anomalies

Build the opportunity matrix

Train and compare models (including tuned Random Forest & stacking ensemble)

Save all outputs

Run the notebook cells sequentially to reproduce the best model (stacking, R²≈0.40).

🖥️ 7. Launch the Dashboard
bash
streamlit run dashboards/app.py
Opens a browser at http://localhost:8501.
The dashboard reads the latest models and matrix from the models/ folder.

🌐 8. Start the REST API
bash
uvicorn api.main:app --reload --port 8000
Swagger docs at http://localhost:8000/docs.

Key endpoints:

POST /predict/value – forecast a contract value

GET /opportunities – top bidding opportunities

GET /anomalies – flagged contracts

GET /benchmarks – value benchmarks by sector/province

📊 9. Generate the Excel Report
Automatic: The scheduler does it for you (next section).
Manual: either run the reporter directly:

bash
python reports/excel_reporter.py
or call the function from your notebook:

python
from reports.excel_reporter import generate_excel_report
# ... (load df, matrix, brief)
output_path = generate_excel_report(df, matrix, brief)
The resulting workbook contains six polished sheets: Executive Summary, Opportunity Matrix, Heatmap, Anomaly Register, Value Benchmarks, and Model Performance.

⏰ 10. Automated Scheduling
The system includes a light‑weight job runner that keeps everything up‑to‑date.

bash
python scheduler/job_runner.py
This starts a background process with three jobs on SAST time:

Job	Frequency	What it does
Full pipeline retrain	Sunday, 02:00	Reloads data, retrains all models, rebuilds matrix, generates Excel report
Anomaly refresh	Mon‑Sat, 06:00	Re‑scans for new governance flags
Matrix rebuild	Wednesday, 08:00	Updates opportunity scores
You can also trigger any job manually:

bash
python scheduler/job_runner.py --run-now full
python scheduler/job_runner.py --run-now anomaly
python scheduler/job_runner.py --run-now matrix
The scheduler uses the real data file data/master_with_anomalies.csv (automatically created by the pipeline). No live DB connection is required for the scheduled jobs – they read from the cleaned CSV.

📈 Model Performance (Latest Run)
After smart filtering and feature engineering (cyclic month, buyer‑category interactions, outlier capping), the final stacking ensemble achieved:

Model	R² (test)	CV R² (mean)	MAE (log)
Stacking	0.3988	0.3759	1.574
Ensemble	0.3919	0.3807	1.554
Random Forest	0.3793	0.3619	1.615
XGBoost	0.3667	0.3549	1.579
LightGBM	0.3571	0.3471	1.582
Ridge	0.1740	0.1510	1.992
Top features: duration_days, buyer_cat_count, duration_band, year, category.

📂 Project Structure
text
procurement_intelligence/
├── config.py                     # DB, thresholds, paths
├── train_pipeline.py             # Master training script
├── requirements.txt
│
├── utils/
│   ├── db_loader.py              # MySQL → pandas (award & contract queries)
│   ├── feature_engineering.py    # Encoders, feature matrix builder
│   └── generate_sample_data.py   # Synthetic data for testing
│
├── models/
│   ├── value_forecaster.py       # RF, XGB, LGB, Ridge, VotingRegressor
│   ├── opportunity_matrix.py     # Province × sector × quarter scoring
│   ├── anomaly_detector.py       # 7 governance flags + statistical tests
│   ├── recommendation_engine.py  # Supplier/official/policy briefs
│
├── dashboard/
│   └── app.py                    # 5‑page Streamlit dashboard
│
├── api/
│   └── main.py                   # FastAPI (10 endpoints)
│
├── reports/
│   └── excel_reporter.py         # 6‑sheet formatted Excel report
│
├── scheduler/
│   └── job_runner.py             # Weekly/daily background jobs
│
├── tests/
│   └── test_pipeline.py          # 30 pytest unit tests
│
├── notebooks/
│   └── ml.ipynb                  # Interactive end‑to‑end analysis
│
├── data/                         # (auto‑created) cleaned CSVs
├── models/                       # (auto‑created) trained .pkl files
└── reports/                      # (auto‑created) outputs
🔍 Key EDA Findings Embedded
Extreme value skew – median ~R943k, mean heavily inflated → all reporting uses medians.

August is the peak month; Dec‑Jan dormant.

Gauteng has high volume but low average value – flagged in the opportunity matrix.

KwaZulu‑Natal Works in Q2 is the single best opportunity cell.

Contract duration follows a bimodal pattern (2‑3 years or <3 months).

🚨 Anomaly Flags
The system automatically tags contracts with:

Flag	Condition	Severity
FLAG 1	Restricted method + value > R15M	2
FLAG 2	Non‑open mega‑contract > R100M	4
FLAG 3	New supplier + value > R50M	5
FLAG 4	Any contract > R200M	3
FLAG 5	Z‑score > 3 within group	2
FLAG 6	Supplier > 30% of category spend	3
FLAG 7	Quarterly spend > 2× provincial average	1
🛠️ Technologies Used
Data & Analysis: Python, pandas, NumPy, MySQL, SQLAlchemy

Machine Learning: scikit‑learn, XGBoost, LightGBM, joblib

Dashboard & API: Streamlit, Plotly, FastAPI, uvicorn, Pydantic

Reporting: openpyxl

Scheduling: APScheduler

Testing: pytest

📌 Licence & Contact
This project is provided as an open‑source reference implementation. For questions or customisation, please open an issue.

Built as a full‑stack procurement intelligence solution for South Africa’s eTender system.

text

Copy the entire content above and paste it into your `README.md` file – it’s ready to go.

update this to generate with full data 
