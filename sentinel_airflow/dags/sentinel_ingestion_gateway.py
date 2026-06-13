from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
from jsonschema import validate, ValidationError

RAW_DATA_PATH = '/tmp/sentinel_raw_payload.json'
CONTRACT_PATH = '/mnt/d/Project_Sentinel/sentinel_airflow/contracts/ecommerce_contract.json'

def generate_simulated_payload():
    """Generates a nested JSON payload simulating an e-commerce order."""
    payload = {
        "event_id": "EVT-104992",
        "order_id": "ORD-88210",
        "customer_id": 8821,
        "checkout_type": "registered",
        "device_type": "iOS App",
        "payment_gateway": "Stripe",
        "acquisition_channel": "IG_Ad_Tech_Retargeting",
        "shipping_address": {
            "city": "Dehradun",
            "state": "Uttarakhand",
            "country": "IN",
            "pin_code": "248001"
        },
        "line_items": [
            {
                "line_item_id": "LI-9921A",
                "item_sku": "TECH-MAC-M5-PRO",
                "category": "Electronics",
                "brand": "Apple",
                "supplier": "Foxconn_Shenzhen",
                "unit_price": 245000.00,
                "quantity": 1,
                "fulfillment_center": "FC_Delhi_North",
                "shipping_carrier": "BlueDart",
                "item_status": "Shipped",
                "promo_code_applied": None,
                "return_reason": None
            }
        ],
        "order_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    with open(RAW_DATA_PATH, 'w') as f:
        json.dump([payload], f, indent=4)
    print(f"Payload generated at {RAW_DATA_PATH}")
    return RAW_DATA_PATH

def enforce_data_contract(ti):
    """Mechanically evaluates the payload against the strict JSON schema."""
    raw_data_path = ti.xcom_pull(task_ids='generate_simulated_payload')
    
    with open(raw_data_path, 'r') as f:
        payloads = json.load(f)
    with open(CONTRACT_PATH, 'r') as f:
        contract = json.load(f)
        
    print("Commencing strict contract validation...")
    for payload in payloads:
        try:
            validate(instance=payload, schema=contract)
        except ValidationError as e:
            raise ValueError(f"CRITICAL FAULT: Payload breached data contract. {e.message}")
    print("Validation successful. Payload matches target architecture.")

def load_and_resolve_schema(ti):
    """Detects schema drift dynamically and loads data into PostgreSQL."""
    raw_data_path = ti.xcom_pull(task_ids='generate_simulated_payload')
    with open(raw_data_path, 'r') as f:
        payloads = json.load(f)
    payload = payloads[0]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    
    # 1. Ensure the raw schema and base table exist
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS raw;")
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS raw.daily_orders (
            _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # 2. Query Postgres for existing columns
    columns_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'raw' AND table_name = 'daily_orders';
    """
    existing_cols = [row[0] for row in pg_hook.get_records(columns_query)]
    
    # 3. Schema Drift Auto-Resolution Engine
    for key, value in payload.items():
        if key not in existing_cols:
            # Map Python data types to strict PostgreSQL types
            if isinstance(value, int):
                pg_type = 'BIGINT'
            elif isinstance(value, float):
                pg_type = 'NUMERIC'
            elif isinstance(value, (dict, list)):
                pg_type = 'JSONB'
            else:
                pg_type = 'TEXT'
            
            drift_sql = f"ALTER TABLE raw.daily_orders ADD COLUMN {key} {pg_type};"
            print(f"Schema Drift Detected! Automatically executing: {drift_sql}")
            pg_hook.run(drift_sql)
            
    # 4. Insert the validated payload
    cols = list(payload.keys())
    vals = []
    for k in cols:
        val = payload[k]
        # Nested JSON must be converted to strings for the Postgres JSONB columns
        if isinstance(val, (dict, list)):
            vals.append(json.dumps(val))
        else:
            vals.append(val)
            
    col_string = ", ".join(cols)
    val_placeholders = ", ".join(["%s"] * len(vals))
    insert_sql = f"INSERT INTO raw.daily_orders ({col_string}) VALUES ({val_placeholders});"
    
    pg_hook.run(insert_sql, parameters=tuple(vals))
    print("Payload successfully self-healed and loaded into raw.daily_orders")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'sentinel_shift_left_gateway',
    default_args=default_args,
    description='Phase 2: Data Contract Gateway & Ingestion',
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 13),
    catchup=False,
    tags=['sentinel', 'ingestion'],
) as dag:

    generate_payload_task = PythonOperator(
        task_id='generate_simulated_payload',
        python_callable=generate_simulated_payload,
    )

    validate_contract_task = PythonOperator(
        task_id='enforce_data_contract',
        python_callable=enforce_data_contract,
    )

    load_data_task = PythonOperator(
        task_id='load_and_resolve_schema',
        python_callable=load_and_resolve_schema,
    )

    # The Final Phase 2 Execution Graph
    generate_payload_task >> validate_contract_task >> load_data_task