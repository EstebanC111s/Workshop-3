# src/producer_features.py
from pathlib import Path
import os, json, time
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from dotenv import load_dotenv

BASE = Path.cwd().resolve()
load_dotenv(BASE / ".env")

DATA_DIR       = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR        = Path(os.getenv("RAW_DIR", DATA_DIR / "raw"))
PROCESSED_DIR  = Path(os.getenv("PROCESSED_DIR", DATA_DIR / "processed"))
ARTIF_DIR      = PROCESSED_DIR / "artifacts"

ROW_ID       = os.getenv("ROW_ID", "row_id")
TARGET       = os.getenv("TARGET", "Happiness Score")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "host.docker.internal:9087")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "happiness_features")
SLEEP_MS     = float(os.getenv("SLEEP_MS", "2"))  # ms entre mensajes

def read_and_unify():
    csvs = sorted(RAW_DIR.glob("*.csv"))
    if len(csvs) < 5:
        raise FileNotFoundError(f"Se esperaban ≥5 CSV en {RAW_DIR}, encontrados: {len(csvs)}")
    parts = []
    for p in csvs:
        d = pd.read_csv(p)
        d["source_file"] = p.name
        if "Year" in d.columns and "source_year" not in d.columns:
            d["source_year"] = d["Year"]
        parts.append(d)
    df = pd.concat(parts, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]
    if ROW_ID not in df.columns:
        df = df.reset_index(drop=False).rename(columns={"index": ROW_ID})
    return df

def etl(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).stripped = df[c].str.strip()
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
    return df

def attach_flags(df: pd.DataFrame) -> pd.DataFrame:
    split_path = ARTIF_DIR / "split_index.csv"
    if not split_path.exists():
        # si no existe, todos 0 (pero idealmente siempre debe existir)
        df["is_train"] = 0
        df["is_test"] = 0
        return df

    split = pd.read_csv(split_path)
    merged = df.merge(split, on=ROW_ID, how="left")

    # rellenar sin info (p.ej. filas nuevas) como 0/0
    merged["is_train"] = merged["is_train"].fillna(0).astype(int)
    merged["is_test"]  = merged["is_test"].fillna(0).astype(int)

    # verificación: no permitir 1/1
    bad = (merged["is_train"] + merged["is_test"] > 1).sum()
    if bad > 0:
        raise ValueError("Hay filas con is_train + is_test > 1 después del merge en producer.")
    return merged

def select_features(df: pd.DataFrame) -> pd.DataFrame:
    fl = ARTIF_DIR / "feature_list.json"
    if fl.exists():
        feature_cols = json.loads(fl.read_text(encoding="utf-8"))
        keep = [ROW_ID] + feature_cols

        # meta + flags
        for col in ["source_file", "source_year", "is_train", "is_test"]:
            if col in df.columns:
                keep.append(col)
        # opcional y_true
        if TARGET in df.columns:
            keep.append(TARGET)

        # asegurar columnas de features aunque falten
        for c in feature_cols:
            if c not in df.columns:
                df[c] = np.nan

        df = df[keep]
    return df

def main():
    df = read_and_unify()
    df = etl(df)
    df = attach_flags(df)
    df = select_features(df)

    print("[producer] DF para streaming:", df.shape)
    print("[producer] columnas:", df.columns.tolist())

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    sleep = SLEEP_MS / 1000.0
    sent = 0
    for _, r in df.iterrows():
        msg = r.to_dict()
        # cast explícito por seguridad:
        msg["is_train"] = int(msg.get("is_train", 0))
        msg["is_test"]  = int(msg.get("is_test", 0))
        producer.send(KAFKA_TOPIC, value=msg)
        sent += 1
        if sleep > 0:
            time.sleep(sleep)

    producer.flush()
    print(f"[producer] enviados {sent} mensajes a {KAFKA_TOPIC}")

if __name__ == "__main__":
    main()
