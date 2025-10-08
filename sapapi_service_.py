import requests
import psycopg2
import time
import logging
from psycopg2 import OperationalError, DatabaseError

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'xxx',
    'user': 'xxx',
    'password': 'xxx'
}

API_BASE_URL = "https://pvp-local-api-test/api/sap-orders/getIdGreaterThan/"
POLL_INTERVAL = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("sap_api_service.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Connected to PostgreSQL.")
        return conn
    except OperationalError as e:
        logging.critical(f"Cannot connect to database: {e}")
        return None

def get_last_id(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(pvpedge_sapapi_lastid), 0) FROM pvpedge_sapapi;")
            last_id = cur.fetchone()[0]
            return last_id
    except DatabaseError as e:
        logging.error(f"Error fetching lastId: {e}")
        return 0

def fetch_new_orders_from_api(last_id):
    url = f"{API_BASE_URL}{last_id}"
    try:
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        return None

def insert_order_to_db(conn, record, new_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pvpedge_sapapi (
                    pvpedge_sapapi_lastid,
                    pvpedge_sapapi_sapLineNo,
                    pvpedge_sapapi_sapIndex,
                    pvpedge_sapapi_sapPackaging,
                    pvpedge_sapapi_sapContent,
                    pvpedge_sapapi_sapBatch,
                    pvpedge_sapapi_sapCount,
                    pvpedge_sapapi_sapOrder,
                    pvpedge_sapapi_sapEan,
                    pvpedge_sapapi_sapProdDate,
                    pvpedge_sapapi_sapTStamp,
                    pvpedge_sapapi_sapConfirm,
                    pvpedge_sapapi_sapPalletNumber,
                    pvpedge_sapapi_handlingUnitLabelCode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pvpedge_sapapi_handlingUnitLabelCode) DO NOTHING;
            """, (
                new_id,
                record.get("lineNo"),
                record.get("index"),
                record.get("packaging"),
                None,  # content not provided
                record.get("batch"),
                record.get("count"),
                record.get("order"),
                record.get("ean"),
                record.get("prodDate"),
                None,  # tStamp not provided
                None,  # confirm not provided
                record.get("palletNumber"),
                record.get("handlingUnitLabelCode")
            ))
            conn.commit()
            logging.info(f"Inserted handlingUnitLabelCode={record.get('handlingUnitLabelCode')}")
    except DatabaseError as e:
        logging.error(f"DB insert error: {e}")
        conn.rollback()

def main():
    conn = connect_db()
    if not conn:
        return

    try:
        while True:
            last_id = get_last_id(conn)
            logging.info(f"Polling API from lastId={last_id} ...")
            api_data = fetch_new_orders_from_api(last_id)
            if api_data and api_data.get("ok") and api_data.get("list"):
                new_id = last_id + 1
                for record in api_data["list"]:
                    insert_order_to_db(conn, record, new_id)
                    new_id += 1
            else:
                logging.info("No new data from API.")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logging.info("Stopping sapapi_service (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Unexpected main loop error: {e}")
    finally:
        try:
            conn.close()
            logging.info("Database connection closed.")
        except Exception:
            pass

if __name__ == "__main__":
    main()
