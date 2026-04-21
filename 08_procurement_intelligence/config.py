# config.py
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
REPORT_DIR = BASE_DIR / "reports"

DATA_DIR.mkdir(exist_ok=True)
MODEL_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

# MySQL connection (edit or set env variables)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "toni_$k8"),
    "database": os.getenv("DB_NAME", "procurement_intelligence"),
    "charset": "utf8mb4",
}

# ML settings
RANDOM_STATE = 42
TEST_SIZE = 0.20
CV_FOLDS = 5
MIN_SAMPLE_SIZE = 5

# Anomaly thresholds (ZAR)
ANOMALY_THRESHOLDS = {
    "restricted_above": 15_000_000,
    "nonopen_mega": 100_000_000,
    "new_supplier_large": 50_000_000,
    "mega_contract": 200_000_000,
    "zscore_cutoff": 3.0,
    "iqr_multiplier": 3.0,
}

# Category mapping
CATEGORY_MAP = {"works": "Works", "goods": "Goods", "services": "Services", "": "Unknown"}

# SA Provinces
SA_PROVINCES = [
    "Gauteng", "KwaZulu-Natal", "Western Cape",
    "Eastern Cape", "Limpopo", "Mpumalanga",
    "North West", "Free State", "Northern Cape",
]

QUARTER_LABELS = {1: "Q1 (Jan-Mar)", 2: "Q2 (Apr-Jun)",
                3: "Q3 (Jul-Sep)", 4: "Q4 (Oct-Dec)"}
