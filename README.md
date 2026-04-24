🏛 South Africa Public Procurement Intelligence System

End-to-end machine learning & business intelligence platform for eTender data (OCDS format)  
Powered by MySQL · Streamlit · FastAPI · Scikit-learn

📌 Overview

This project transforms raw public procurement data into actionable intelligence across 43,000+ tenders and 8,000+ awards.

It enables:

Suppliers → Identify where and when to bid, and estimate contract values  
Procurement officials → Detect governance risks and anomalies  
Policy analysts → Understand spending patterns and market concentration

The system combines machine learning, analytics, and automation into a production-style pipeline.

✨ Key Features
📊 Interactive Dashboard (Streamlit) – Multi-page analytics app  
🌐 REST API (FastAPI) – 10 endpoints for predictions and insights  
🤖 ML Forecasting – Random Forest, XGBoost, LightGBM, Ensemble models  
📈 Full Dataset Training – Uses all awards data (no filtering)  
🚨 Anomaly Detection – 7 governance risk flags  
🧭 Opportunity Matrix – Province × Sector × Quarter scoring  
📄 Excel Reporting – Automated 6-sheet reports  
⏰ Scheduler – Automated retraining and monitoring  
🛢 MySQL Integration – Direct OCDS pipeline  
🧪 Synthetic Data Support – Run without a database  

📊 Architecture
Flow Overview
MySQL (OCDS Staging Tables)
        ↓
Data Loading & EDA
        ↓
Feature Engineering
        ↓
Anomaly Detection + Opportunity Matrix
        ↓
Machine Learning Pipeline
        ↓
Trained Models (.pkl)
        ↓
-----------------------------------------
|   Dashboard   |    API    |  Reports   |
|  Streamlit    | FastAPI   |   Excel    |
-----------------------------------------
        ↓
Scheduler (Automation & Monitoring)

📁 Data
Primary Source: MySQL database (procurement_intelligence)  
Tables: main_staging, awards_staging, contracts_staging  
Fallback: Synthetic data (utils/generate_sample_data.py)  
Training Approach  
Uses full dataset (~5,600+ awards)  
No value filtering applied  
Covers full spectrum:  
Small contracts  
Medium tenders  
Large awards  

Result: More realistic, production-ready predictions  

🚀 Quick Start
1. Clone Repository
git clone <repo-url>
cd procurement_intelligence
2. Create Environment
conda create -n procurement python=3.11
conda activate procurement
3. Install Dependencies
pip install -r requirements.txt
4. Configure Database

Edit config.py:

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "procurement_intelligence",
    "charset": "utf8mb4",
}
🧪 Run the Pipeline
With MySQL
python train_pipeline.py
Without MySQL (Synthetic Data)
python utils/generate_sample_data.py
python train_pipeline.py --skip-db
Outputs
models/ → trained models  
reports/ → CSV / Excel outputs  
data/master_with_anomalies.csv → cleaned dataset  

📓 Notebook (Exploration)

Open:

notebooks/ml.ipynb

Includes:

Data loading  
Feature engineering  
Model training  
Evaluation and comparison  

🖥️ Dashboard
streamlit run dashboard/app.py

Open in browser: http://localhost:8501

🌐 API
uvicorn api.main:app --reload --port 8000

Docs: http://localhost:8000/docs

Key Endpoints
POST /predict/value – Predict contract value  
GET /opportunities – Top opportunities  
GET /anomalies – Flagged contracts  
GET /benchmarks – Sector/province benchmarks  

📊 Excel Reports
python reports/excel_reporter.py

Includes:

Executive Summary  
Opportunity Matrix  
Heatmap  
Anomaly Register  
Benchmarks  
Model Performance  

⏰ Scheduler
python scheduler/job_runner.py
Job	Frequency	Description  
Full retrain	Sunday 02:00	Retrains models  
Anomaly scan	Mon–Sat 06:00	Updates risk flags  
Matrix refresh	Wednesday 08:00	Updates opportunity scores  

📈 Model Notes
Trained on full dataset (no filtering)  
Handles full market distribution  
Strong generalisation across contract sizes  

Trade-off:

Slightly lower R² vs filtered models  
Better real-world applicability  

🔍 Key Insights
Strong right-skew in values → medians preferred  
Procurement peaks in August, slows Dec–Jan  
Provincial differences in volume vs value  
Contract durations show bimodal distribution  

🚨 Anomaly Flags
Flag	Description  
1	Restricted method + high value  
2	Non-open mega contract  
3	New supplier large award  
4	Extreme high-value contract  
5	Statistical outlier  
6	Supplier concentration  
7	Provincial spend spike  

🛠 Tech Stack
Python, pandas, NumPy  
MySQL, SQLAlchemy  
scikit-learn, XGBoost, LightGBM  
Streamlit, Plotly  
FastAPI, uvicorn  
openpyxl  
APScheduler  
pytest  

📂 Project Structure
procurement_intelligence/
├── config.py
├── train_pipeline.py
├── utils/
├── models/
├── dashboard/
├── api/
├── reports/
├── scheduler/
├── notebooks/
├── tests/

📜 License

Open-source reference implementation.

Built as a full-stack procurement intelligence system for South Africa’s eTender ecosystem
