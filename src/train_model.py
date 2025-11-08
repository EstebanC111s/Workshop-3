# src/train_model.py
from pathlib import Path
import os, json
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
import joblib

# ===== Setup =====
BASE = Path.cwd().resolve()
load_dotenv(BASE / ".env")

DATA_DIR       = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR        = Path(os.getenv("RAW_DIR", DATA_DIR / "raw"))
INTER_DIR      = DATA_DIR / "intermediate"
PROCESSED_DIR  = Path(os.getenv("PROCESSED_DIR", DATA_DIR / "processed"))
ARTIF_DIR      = PROCESSED_DIR / "artifacts"
REPORTS_DIR    = PROCESSED_DIR / "reports"

for d in (INTER_DIR, PROCESSED_DIR, ARTIF_DIR, REPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

ROW_ID = os.getenv("ROW_ID", "row_id")
TARGET = os.getenv("TARGET", "Happiness Score")

# ===== 1) Leer 5 CSV y unificar =====
csvs = sorted(RAW_DIR.glob("*.csv"))
if len(csvs) < 5:
    raise FileNotFoundError(f"Se esperaban ≥5 CSV en {RAW_DIR}, encontrados: {len(csvs)}")

parts = []
for p in csvs:
    df = pd.read_csv(p)
    df["source_file"] = p.name
    if "Year" in df.columns and "source_year" not in df.columns:
        df["source_year"] = df["Year"]
    parts.append(df)

df_uni = pd.concat(parts, ignore_index=True)
df_uni.columns = [c.strip() for c in df_uni.columns]
df_uni = df_uni.reset_index(drop=False).rename(columns={"index": ROW_ID})
df_uni.to_csv(INTER_DIR / "unified.csv", index=False)

# ===== 2) ETL para entrenamiento =====
df = df_uni.copy()

for c in df.columns:
    if df[c].dtype == "object":
        df[c] = df[c].astype(str).str.strip()

for c in df.columns:
    if df[c].dtype == "object":
        conv = pd.to_numeric(df[c], errors="coerce")
        if conv.notna().mean() >= 0.90:
            df[c] = conv

for c in df.select_dtypes(include=[np.number]).columns:
    if df[c].isna().any():
        df[c] = df[c].fillna(df[c].median())

for c in df.select_dtypes(include=["object"]).columns:
    if df[c].isna().any():
        df[c] = df[c].fillna("Unknown")

# (EDA a reports si quieres; omito por brevedad)

# ===== 3) Selección de features =====
NUM_FEATS_DEFAULT = [
    "GDP per Capita","Social support","Healthy life expectancy",
    "Freedom to make life choices","Generosity","Perceptions of corruption"
]
CAT_FEATS_DEFAULT = ["Country","Region"]

num_feats = [c for c in NUM_FEATS_DEFAULT if c in df.columns]
cat_feats = [c for c in CAT_FEATS_DEFAULT if c in df.columns]
feature_cols = num_feats + cat_feats
(ARTIF_DIR / "feature_list.json").write_text(json.dumps(feature_cols, indent=2), encoding="utf-8")

# ===== 4) Split + flags 1/0 =====
assert TARGET in df.columns, f"No encuentro la variable objetivo '{TARGET}'"

X = df[feature_cols].copy()
y = df[TARGET].astype(float).copy()

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.30, random_state=42, shuffle=True
)

split_train = pd.DataFrame({
    ROW_ID: X_train.index,
    "is_train": 1,
    "is_test": 0
})
split_test = pd.DataFrame({
    ROW_ID: X_test.index,
    "is_train": 0,
    "is_test": 1
})
split_idx = pd.concat([split_train, split_test], ignore_index=True)

# check: siempre 1 u 0, nunca otra cosa
assert ((split_idx["is_train"] + split_idx["is_test"]) == 1).all(), "Error en flags de split"

split_idx.to_csv(ARTIF_DIR / "split_index.csv", index=False)

# ===== 5) Entrenar modelo =====
pre = ColumnTransformer(
    transformers=[
        ("num", Pipeline([("scaler", StandardScaler())]), num_feats),
        ("cat", Pipeline([("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_feats),
    ],
    remainder="drop"
)
pipe = Pipeline([("pre", pre), ("model", LinearRegression())])
pipe.fit(X_train, y_train)

y_pred = pipe.predict(X_test)
r2 = r2_score(y_test, y_pred)
rmse = mean_squared_error(y_test, y_pred, squared=False)
print(f"R2={r2:.3f}  RMSE={rmse:.3f}")

joblib.dump(pipe, ARTIF_DIR / "model.pkl")
print("Artefactos guardados en:", ARTIF_DIR)
