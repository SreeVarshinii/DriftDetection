import time
import json
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from datetime import datetime

# Database config
DB_CONFIG = {
    "dbname": "olist_db",
    "user": "postgres",
    "password": "olist",
    "host": "localhost",
    "port": "5432"
}

KNOWN_COLUMNS = {
    "orders": {"order_id", "customer_id", "order_status", "emitted_at", "schema_version"},
    "customers": {"customer_id", "customer_state", "customer_city", "emitted_at", "schema_version"},
    "items": {"order_item_id", "order_id", "price", "freight_value", "emitted_at", "schema_version"},
    "payments": {"payment_id", "order_id", "payment_type", "payment_installments", "emitted_at", "schema_version"}
}

SCHEMA_REGISTRY_FILE = "schema_registry.json"

class OlistHandler(FileSystemEventHandler):
    def __init__(self):
        self.registry = self._load_registry()

    def _load_registry(self):
        if Path(SCHEMA_REGISTRY_FILE).exists():
            with open(SCHEMA_REGISTRY_FILE, "r") as f:
                return json.load(f)
        return {
            "orders": {"hash": None, "columns": []},
            "customers": {"hash": None, "columns": []},
            "items": {"hash": None, "columns": []},
            "payments": {"hash": None, "columns": []}
        }

    def _save_registry(self):
        with open(SCHEMA_REGISTRY_FILE, "w") as f:
            json.dump(self.registry, f, indent=2)

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".json"):
            return
        
        filepath = Path(event.src_path)
        time.sleep(0.1)
        self.process_file(filepath)

    def process_file(self, filepath):
        try:
            filename = filepath.name
            entity_type = filename.split("_")[0]
            
            if entity_type not in KNOWN_COLUMNS:
                filepath.unlink()
                return

            try:
                df = pd.read_json(filepath, orient="records", lines=True)
            except ValueError:
                df = pd.read_json(filepath, typ='series').to_frame().T
            
            columns = sorted(df.columns.tolist())
            schema_str = ",".join(columns)
            schema_hash = hashlib.md5(schema_str.encode()).hexdigest()
            
            drift_msg = ""
            
            if entity_type not in self.registry:
                self.registry[entity_type] = {"hash": None, "columns": []}
                
            last_schema = self.registry[entity_type]
            
            if last_schema["hash"] != schema_hash:
                old_cols = set(last_schema["columns"])
                new_cols = set(columns)
                added = new_cols - old_cols
                dropped = old_cols - new_cols
                
                if last_schema["hash"] is not None:
                    drift_parts = []
                    if added: drift_parts.append(f"added: {','.join(added)}")
                    if dropped: drift_parts.append(f"dropped: {','.join(dropped)}")
                    drift_msg = f" | {' | '.join(drift_parts)}"
                
                self.registry[entity_type] = {
                    "hash": schema_hash,
                    "columns": columns,
                    "version": last_schema.get("version", 0) + 1
                }
                self._save_registry()
            
            schema_version = self.registry[entity_type].get("version", 1)
            
            if drift_msg:
                print(f"[DRIFT] {filename} — v{schema_version}{drift_msg}")
            else:
                print(f"[OK] {filename} — v{schema_version}")
            
            self._load_to_postgres(df.iloc[0].to_dict(), schema_version, entity_type)
            filepath.unlink()
            
        except Exception as e:
            print(f"Error processing {filepath.name}: {e}")

    def _load_to_postgres(self, record, schema_version, entity_type):
        base_record = {}
        rescued_data = {}
        valid_cols = KNOWN_COLUMNS[entity_type]
        
        for k, v in record.items():
            if k in valid_cols:
                base_record[k] = v
            else:
                if pd.isna(v): rescued_data[k] = None
                else: rescued_data[k] = v
                
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        try:
            columns = list(base_record.keys())
            values = list(base_record.values())
            
            values = [None if pd.isna(v) else v for v in values]
            
            columns.extend(["_rescued_data", "_schema_version"])
            values.extend([Json(rescued_data), schema_version])
            
            pk_col = {
                "orders": "order_id",
                "customers": "customer_id",
                "items": "order_item_id",
                "payments": "payment_id"
            }[entity_type]
            
            query = f"""
                INSERT INTO bronze.{entity_type} ({','.join(columns)})
                VALUES ({','.join(['%s'] * len(values))})
                ON CONFLICT ({pk_col}) DO NOTHING;
            """
            cur.execute(query, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

if __name__ == "__main__":
    path = "./watch"
    Path(path).mkdir(exist_ok=True)
    
    event_handler = OlistHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    print(f"Watching {path} for 4-table entity files...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
