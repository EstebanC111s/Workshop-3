# 📊 Workshop-3 — Happiness Score ETL + ML + Kafka + Postgres + Power BI

This repository contains the full solution for **Workshop-3**.

We implement an end-to-end pipeline that:

- Ingests **5 yearly CSV files** (World Happiness dataset).
- Performs **EDA + ETL** to create a unified, clean dataset.
- Trains an **interpretable regression model** to predict the *Happiness Score*.
- Streams features through **Apache Kafka** (Python Producer).
- Consumes messages, generates predictions, and loads them into a **PostgreSQL Data Warehouse** (Python Consumer).
- Connects **Power BI** directly to the DW to build **KPIs & visualizations** for model evaluation.

The implementation follows the block diagram:

- **Top box** → CSVs → EDA/ETL → model training → feature selection → data streaming.
- **Bottom box** → Kafka consumer → model prediction → database load → analytics layer.

---

## 🧠 ETL & Modeling Flow

| ⚙️ Step                         | 🔍 Description |
|---------------------------------|----------------|
| 📥 **Extract**                  | Read 2015–2019 CSV files from `data/raw/` |
| 🐍 **Transform (EDA + ETL)**    | Standardize schemas, select relevant columns, handle basic quality checks, and concatenate into a unified dataset |
| 📦 **Unified Dataset**         | Persist `unified.csv` in `data/processed/` as the single source of truth for training |
| 🧮 **Feature Selection**        | Choose socio-economic drivers + Country/Region as model inputs |
| 🧪 **Model Training**           | Train a regression model (Ridge/Linear) to predict Happiness Score, export `model.pkl` + `feature_list.json` + `split_index.csv` |
| 📡 **Kafka Producer (Features)**| Read & transform records, attach `row_id`, `is_train`, `is_test`, and send feature messages to Kafka topic `happiness_features` |
| 📥 **Kafka Consumer**           | Subscribe to topic, rebuild features, load `model.pkl`, generate predictions `y_pred` |
| 🗄️ **DW Load (Postgres)**       | Insert results into `public.fact_predictions` (features + `y_true` + `y_pred` + flags) |
| 📊 **BI Layer (Power BI)**      | Connects to Postgres, builds KPIs (R², RMSE, MAE, etc.) and dashboards on top of `fact_predictions` |

---

## 📁 Project Structure

```bash
Workshop-3/
├─ src/
│  ├─ train_model.py          # EDA/ETL-to-training: unified dataset, split, model, artifacts
│  ├─ producer_features.py    # Kafka producer: streams feature records from CSVs
│  └─ consumer_dw.py          # Kafka consumer: predicts and loads into Postgres DW
│
├─ notebooks/
│  └─ EDA_unified.ipynb       # (optional) Exploratory + ETL notebook used to design the pipeline
│
├─ data/
│  ├─ raw/
│  │  ├─ 2015.csv
│  │  ├─ 2016.csv
│  │  ├─ 2017.csv
│  │  ├─ 2018.csv
│  │  └─ 2019.csv
│  └─ processed/
│     ├─ unified.csv          # unified happiness dataset
│     └─ artifacts/
│        ├─ model.pkl         # trained regression model (ignored in git)
│        ├─ feature_list.json # expected feature order at serving time
│        └─ split_index.csv   # train/test assignment per row_id
│
├─ powerbi/
│  └─ Happiness_Dashboard.pbix # (optional) Power BI report connected to Postgres
│
├─ docker-compose.yml          # Kafka + Zookeeper + Postgres + Kafka UI
├─ requirements.txt
├─ .env                        # environment variables (not committed)
├─ .gitignore
└─ README.md
```
# ⚙️ Configuration
```
# Kafka
KAFKA_BROKER=localhost:9087
KAFKA_TOPIC=happiness_features

# Postgres DW
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dw_happiness
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Paths
DATA_DIR=./data
RAW_DIR=./data/raw
PROCESSED_DIR=./data/processed

# Environment
ENV=dev

```

# 🐳 Run with Docker (Kafka + Postgres stack)

```
# Start Kafka, Zookeeper, Postgres, and Kafka UI
docker compose up -d
```
Typical services:

Kafka on localhost:9087

Kafka UI on http://localhost:<ui-port>

Postgres on localhost:5432 (dw_happiness DB)

# 🚀 End-to-End Pipeline
# 1️⃣ Training phase

Run once (after placing CSVs in data/raw/):
```
# 1) Create unified dataset + train model + export artifacts
python -m src.train_model

```

# 2️⃣ Streaming phase (Producer)

With Docker stack running:

```
python -m src.producer_features
```

# 3️⃣ Serving + DW Load phase (Consumer)

In another terminal (stack running, model artifacts available):

```
python -m src.consumer_dw


```
