import os
import json
from pathlib import Path

import pandas as pd
import joblib
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === Cargar .env y rutas ===
BASE = Path.cwd().resolve()
load_dotenv(BASE / ".env")

DATA_DIR       = Path(os.getenv("DATA_DIR", "./data"))
PROCESSED_DIR  = Path(os.getenv("PROCESSED_DIR", DATA_DIR / "processed"))
ARTIF_DIR      = PROCESSED_DIR / "artifacts"

ROW_ID        = os.getenv("ROW_ID", "row_id")
TARGET        = os.getenv("TARGET", "Happiness Score")
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9087")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "happiness_features")
DB_URL        = os.getenv("DB_URL")  # postgresql://...
TABLE_NAME    = os.getenv("DB_TABLE", "fact_predictions")

if not DB_URL:
    raise ValueError("DB_URL no está definido en el .env")

# === Cargar modelo y feature_list ===
model_path = ARTIF_DIR / "model.pkl"
feat_path  = ARTIF_DIR / "feature_list.json"

if not model_path.exists():
    raise FileNotFoundError(f"No se encontró el modelo en {model_path}")
if not feat_path.exists():
    raise FileNotFoundError(f"No se encontró feature_list.json en {feat_path}")

pipe = joblib.load(model_path)
feature_cols = json.loads(feat_path.read_text(encoding="utf-8"))

print("[consumer] Modelo y feature_list cargados.")
print("[consumer] Features esperadas:", feature_cols)

# === Conexión a Postgres ===
engine = create_engine(DB_URL, future=True)

# Crear tabla con las columnas que quieres
with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            row_id      BIGINT,
            country     TEXT,
            year        INT,
            gdp         DOUBLE PRECISION,
            social      DOUBLE PRECISION,
            health      DOUBLE PRECISION,
            freedom     DOUBLE PRECISION,
            corrupt     DOUBLE PRECISION,
            y_true      DOUBLE PRECISION,
            y_pred      DOUBLE PRECISION,
            is_train    INT,
            is_test     INT,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
    """))

print(f"[consumer] Tabla `{TABLE_NAME}` lista en Postgres.")

# === Configurar KafkaConsumer ===
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="etl-workshop-consumer",
)

print(f"[consumer] Escuchando topic `{KAFKA_TOPIC}` en {KAFKA_BROKER}...")

buffer = []
BATCH_SIZE = 200

def to_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None

def to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None

def flush_batch():
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"[consumer] Insertadas {len(df)} filas en {TABLE_NAME}")
    buffer.clear()

try:
    for msg in consumer:
        rec = msg.value  # dict desde producer_features

        # --- campos base ---
        row_id = rec.get(ROW_ID)
        country = rec.get("Country")
        # year: primero 'Year'; si no, 'source_year'; si no, intenta parsear '2015.csv'
        year = rec.get("Year")
        if year is None:
            year = rec.get("source_year")
        if year is None:
            sf = rec.get("source_file")
            if isinstance(sf, str) and len(sf) >= 4 and sf[:4].isdigit():
                year = int(sf[:4])


        # mapear features a nombres cortos
        gdp      = rec.get("GDP per Capita")
        social   = rec.get("Social support")
        health   = rec.get("Healthy life expectancy")
        freedom  = rec.get("Freedom to make life choices")
        corrupt  = rec.get("Perceptions of corruption")

        y_true   = rec.get(TARGET)
        is_train = rec.get("is_train", 0)
        is_test  = rec.get("is_test", 0)

        # --- armar X para el modelo con feature_cols originales ---
        x_dict = {col: rec.get(col, None) for col in feature_cols}
        X = pd.DataFrame([x_dict])

        try:
            y_pred = float(pipe.predict(X)[0])
        except Exception as e:
            print(f"[consumer] Error al predecir row_id={row_id}: {e}")
            continue

        buffer.append({
            "row_id":   row_id,
            "country":  country,
            "year":     to_int(year),
            "gdp":      to_float(gdp),
            "social":   to_float(social),
            "health":   to_float(health),
            "freedom":  to_float(freedom),
            "corrupt":  to_float(corrupt),
            "y_true":   to_float(y_true),
            "y_pred":   y_pred,
            "is_train": to_int(is_train) or 0,
            "is_test":  to_int(is_test) or 0,
        })

        if len(buffer) >= BATCH_SIZE:
            flush_batch()

except KeyboardInterrupt:
    print("[consumer] Interrumpido, haciendo flush final...")
finally:
    flush_batch()
    print("[consumer] Cerrado correctamente.")
