import pandas as pd
import json
import time
import random
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import argparse

class OlistSimulator:
    def __init__(self, data_dir="./data", sink_dir="./watch", mode="normal", use_kafka=False):
        self.mode = mode
        self.use_kafka = use_kafka
        self.sink_dir = Path(sink_dir)
        self.sink_dir.mkdir(parents=True, exist_ok=True)
        
        if self.use_kafka:
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers="localhost:9092",
                    value_serializer=lambda v: json.dumps(v).encode()
                )
            except ImportError:
                print("Warning: kafka-python not installed. Falling back to local file sink.")
                self.use_kafka = False
                
        print("Loading datasets...")
        self.orders    = pd.read_csv(f"{data_dir}/olist_orders_dataset.csv")
        self.items     = pd.read_csv(f"{data_dir}/olist_order_items_dataset.csv")
        self.payments  = pd.read_csv(f"{data_dir}/olist_order_payments_dataset.csv")
        self.reviews   = pd.read_csv(f"{data_dir}/olist_order_reviews_dataset.csv")
        self.customers = pd.read_csv(f"{data_dir}/olist_customers_dataset.csv")
        print("Datasets loaded.")

        self.schema_version = 1
        self.events_emitted = 0

    def build_and_emit(self, order_id):
        order = self.orders[self.orders.order_id == order_id].iloc[0].to_dict()
        items = self.items[self.items.order_id == order_id].to_dict("records")
        payment = self.payments[self.payments.order_id == order_id].to_dict("records")
        customer = self.customers[self.customers.customer_id == order["customer_id"]].iloc[0].to_dict()
        
        mutating_now = (self.mode == "schema_mutation" and self.events_emitted % 10 == 0 and self.events_emitted > 0)
        if mutating_now:
            self.schema_version += 1
            mutation = random.choice(["add", "drop", "rename"])
        else:
            mutation = None

        customer_evt = {
            "customer_id": customer["customer_id"],
            "customer_state": customer["customer_state"],
            "customer_city": customer["customer_city"],
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": self.schema_version
        }
        self.emit_event(customer_evt, "customers", customer["customer_id"])

        order_evt = {
            "order_id": order_id,
            "customer_id": order["customer_id"],
            "order_status": order["order_status"],
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": self.schema_version
        }
        if self.mode == "late_arrival":
            order_evt["late_arrival"] = True
            order_evt["original_timestamp"] = "2024-01-15T08:00:00"

        if mutating_now and mutation == "add":
            order_evt["loyalty_tier"] = random.choice(["bronze", "silver", "gold"])
            
        self.emit_event(order_evt, "orders", order_id)

        burst_factor = random.uniform(0.1, 0.3) if self.mode == "burst" else 1.0
        for item in items:
            item_evt = {
                "order_item_id": f"{order_id}_{item['order_item_id']}",
                "order_id": order_id,
                "price": round(item["price"] * burst_factor, 2),
                "freight_value": item["freight_value"],
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "schema_version": self.schema_version
            }
            if self.mode == "burst":
                item_evt["burst_flag"] = True
                
            if mutating_now:
                if mutation == "drop":
                    item_evt.pop("freight_value", None)
                elif mutation == "rename":
                    item_evt["item_price"] = item_evt.pop("price", None)
                    
            self.emit_event(item_evt, "items", item_evt["order_item_id"])

        for p in payment:
            payment_evt = {
                "payment_id": f"{order_id}_{p['payment_sequential']}",
                "order_id": order_id,
                "payment_type": p["payment_type"],
                "payment_installments": p["payment_installments"],
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "schema_version": self.schema_version
            }
            self.emit_event(payment_evt, "payments", payment_evt["payment_id"])

    def emit_event(self, event, entity_type, pk):
        if self.use_kafka:
            self.producer.send(f"olist_{entity_type}", value=event)
        else:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
            filepath = self.sink_dir / f"{entity_type}_{timestamp}_{pk}.json"
            with open(filepath, "w") as f:
                json.dump(event, f)

    def run(self, delay=0.5, limit=None):
        order_ids = self.orders.order_id.sample(frac=1).tolist()

        if limit:
            order_ids = order_ids[:limit]

        for order_id in order_ids:
            try:
                self.build_and_emit(order_id)
                self.events_emitted += 1
                print(f"Emitted {order_id[:8]} across all 4 tables... | v{self.schema_version} | {self.mode}")
                if self.mode == "burst":
                    time.sleep(0.1)
                else:
                    time.sleep(delay)
            except Exception as e:
                print(f"Skipped {order_id}: {e}")
                continue
                
        # Automatically generate the Living Data Map if we mutated the schema
        if self.mode == "schema_mutation":
            import subprocess
            print("\nWaiting 2 seconds for watcher to catch up...")
            time.sleep(2)
            print("Generating updated Living Data Map...")
            subprocess.run(["python", "schema_viz.py"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist Data Simulator")
    parser.add_argument("--mode", type=str, default="normal", choices=["normal", "burst", "schema_mutation", "late_arrival"])
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--kafka", action="store_true")
    args = parser.parse_args()
    
    simulator = OlistSimulator(mode=args.mode, use_kafka=args.kafka)
    try:
        simulator.run(delay=args.delay, limit=args.limit)
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
