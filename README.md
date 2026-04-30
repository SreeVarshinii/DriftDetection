# 🌊 SchemaDrift: Resilient Data Pipeline

A real-time, event-driven data pipeline that simulates real-world e-commerce data streams (using the Olist dataset), actively monitors for unexpected **schema drift**, and auto-rescues mutated data directly into a dynamic PostgreSQL architecture.

## 🏗️ Architecture
1. **The Simulator (`simulator.py`)**: Replays historical Olist data. Emits 4 normalized entity events (`orders`, `items`, `customers`, `payments`) with controlled delays, bursts, and chaotic schema mutations (dropped columns, renamed columns, new fields).
2. **The Watcher (`watcher.py`)**: A `watchdog` daemon that intercepts incoming JSON events. It infers the schema on-the-fly, computes MD5 hashes, compares them against a local `schema_registry.json`, and logs schema evolution metadata.
3. **The Data Lakehouse (PostgreSQL)**:
   * **Bronze Layer**: 4 tables for each entity. Uses a `_rescued_data` JSONB column to seamlessly absorb unknown/mutated fields without crashing the ingestion pipeline.
   * **Silver Layer**: A massive denormalized view (`silver.cleaned_orders`) that dynamically reconstructs the orders, patching dropped columns and extracting newly mutated fields directly out of the JSONB rescue column.
   * **Gold Layer**: Aggregated business metrics and anomaly tracking (e.g., tracking bot bursts by state and revenue by VIP tiers).
4. **Living Data Map (`schema_viz.py`)**: An interactive `vis.js` graph that parses Postgres schemas and automatically traces the **blast radius** of schema drift downstream through your views.

---

## 🚀 Getting Started

### 1. Start the Database
The pipeline requires a PostgreSQL instance. A `docker-compose.yml` is provided to spin up Postgres and pgAdmin.
```bash
docker-compose up -d
```
*(The schema and views are automatically built via `init.sql` on the first boot!)*

### 2. Setup the Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pandas psycopg2-binary watchdog faker kafka-python
```

### 3. Initialize the Watcher
Start the daemon that monitors the `./watch` directory and safely routes events into Postgres:
```bash
python watcher.py
```

### 4. Run the Simulator
In a separate terminal window, run the simulator. This acts as our chaotic upstream microservice.
```bash
# Standard normal flow
python simulator.py

# Chaos Mode (Inject schema drift every 10 events)
python simulator.py --mode schema_mutation --limit 25
```
*For details on simulating bot attacks or late-arriving dimensions, check out the full docs in `reports/simulator_guide.md`.*

---

## 🗺️ The Living Data Map
Once you've run the simulator and triggered a schema mutation, generate the dynamic lineage map:
```bash
python schema_viz.py
```
This automatically parses your database, checks the drift registry, and opens `docs/schema.html` in your browser. Any Bronze tables hit by schema drift will **glow red**, and all affected downstream Silver and Gold tables will **glow orange**, mapping the blast radius of the mutation!

---

## 📊 Accessing the Data
You can query the pipeline directly from your terminal:
```bash
docker exec -it olist_postgres psql -U postgres -d olist_db
```
Or via the browser using **pgAdmin** at `http://localhost:5050` (`admin@olist.com` / `admin`).
