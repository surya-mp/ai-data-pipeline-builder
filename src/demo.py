from __future__ import annotations

import argparse
import asyncio

from src.models import ContractColumn, DataContract
from src.service import PipelineService


def bootstrap(service: PipelineService) -> None:
    """Create portable local demo sources and their contracts."""
    service.database.execute("CREATE TABLE IF NOT EXISTS customers (customer_id INTEGER PRIMARY KEY, name TEXT NOT NULL, country TEXT NOT NULL)")
    service.database.execute("CREATE TABLE IF NOT EXISTS orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(customer_id), amount REAL NOT NULL)")
    service.database.execute("CREATE TABLE IF NOT EXISTS payments (payment_id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(order_id), status TEXT NOT NULL, paid_amount REAL NOT NULL)")
    with service.database.begin() as connection:
        for statement in (
            "INSERT INTO customers (customer_id, name, country) VALUES (1, 'Ada Lovelace', 'GB') ON CONFLICT (customer_id) DO NOTHING",
            "INSERT INTO customers (customer_id, name, country) VALUES (2, 'Grace Hopper', 'US') ON CONFLICT (customer_id) DO NOTHING",
            "INSERT INTO orders (order_id, customer_id, amount) VALUES (101, 1, 125.0) ON CONFLICT (order_id) DO NOTHING",
            "INSERT INTO orders (order_id, customer_id, amount) VALUES (102, 1, 75.0) ON CONFLICT (order_id) DO NOTHING",
            "INSERT INTO orders (order_id, customer_id, amount) VALUES (103, 2, 250.0) ON CONFLICT (order_id) DO NOTHING",
            "INSERT INTO payments (payment_id, order_id, status, paid_amount) VALUES (1001, 101, 'paid', 125.0) ON CONFLICT (payment_id) DO NOTHING",
            "INSERT INTO payments (payment_id, order_id, status, paid_amount) VALUES (1002, 102, 'paid', 75.0) ON CONFLICT (payment_id) DO NOTHING",
            "INSERT INTO payments (payment_id, order_id, status, paid_amount) VALUES (1003, 103, 'paid', 250.0) ON CONFLICT (payment_id) DO NOTHING",
        ):
            service.database.execute(statement, connection)
    service.save_contract(DataContract(asset="customers", columns=[ContractColumn(name="customer_id", data_type="INTEGER", nullable=False), ContractColumn(name="name", data_type="TEXT", nullable=False), ContractColumn(name="country", data_type="TEXT", nullable=False)], primary_key=["customer_id"]))
    service.save_contract(DataContract(asset="orders", columns=[ContractColumn(name="order_id", data_type="INTEGER", nullable=False), ContractColumn(name="customer_id", data_type="INTEGER", nullable=False), ContractColumn(name="amount", data_type="REAL", nullable=False)], primary_key=["order_id"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap and run the complete local pipeline demo.")
    parser.add_argument("--bootstrap-only", action="store_true")
    args = parser.parse_args()
    service = PipelineService()
    bootstrap(service)
    if args.bootstrap_only:
        print("Demo sources and contracts created.")
        return
    definition = service.create_pipeline("Create a customer-level dataset with total purchase amount, number of orders, and average order value using customers and orders")
    service.approve_pipeline(definition.id, reviewer="demo-bootstrap", note="Automated demo approval.")
    state = asyncio.run(service.run_pipeline(definition.id))
    print(f"Pipeline {definition.id} completed: {state.status}. Report: {service.report_path(definition.id)}")
