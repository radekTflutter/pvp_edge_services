import time
import logging
import sys
import traceback
from datetime import datetime, timezone
import io
import os

from pylogix import PLC

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.db import get_db_connection

# --- CONFIGURATION ---
PLC_IP = "192.168.x.x"
TRIGGER_TAG = "PaletPosition"
LABEL_OK_TAG = "LabelOk"
LABEL_NOK_TAG = "LabelNotOk"

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# --- HELPERS ---
def zulu_timestamp():
    """Return current UTC time in Zulu format without milliseconds."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- DATABASE FUNCTIONS ---
def get_latest_unconfirmed_order():
    """Fetch the latest order that has not been acknowledged by PLC."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pvpedge_orders_id, pvpedge_orders_confirm
                    FROM pvpedge_orders
                    WHERE plc_ack_sent = FALSE
                    ORDER BY pvpedge_orders_id DESC
                    LIMIT 1
                """)
                return cur.fetchone()
    except Exception as e:
        logging.error(f"[DB ERROR] Failed to fetch latest unconfirmed order: {e}")
        return None


def mark_order_acknowledged(order_id):
    """Mark order as acknowledged and set timestamp."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pvpedge_orders
                    SET plc_ack_sent = TRUE,
                        pvpedge_ack_plc_timestamp = %s
                    WHERE pvpedge_orders_id = %s
                """, (zulu_timestamp(), order_id))
                conn.commit()
                logging.info(f"[DB] Order ID={order_id} acknowledged with PLC timestamp")
    except Exception as e:
        logging.error(f"[DB ERROR] Failed to update order ID={order_id}: {e}")


# --- MAIN SERVICE ---
def run_plc_service():
    logging.info(f"[START] plc_service started at {zulu_timestamp()}")

    with PLC() as comm:
        comm.IPAddress = PLC_IP
        previous_trigger = None

        while True:
            try:
                result = comm.Read(TRIGGER_TAG)
                if result.Status != "Success":
                    logging.warning("[PLC] Failed to read trigger tag")
                    time.sleep(1)
                    continue

                trigger = result.Value

                # Rising edge (0 → 1)
                if trigger == 1 and previous_trigger != 1:
                    logging.info("[PLC] Rising edge detected")

                    # Reset tags
                    comm.Write(LABEL_OK_TAG, 0)
                    comm.Write(LABEL_NOK_TAG, 0)

                    order = get_latest_unconfirmed_order()
                    if order:
                        order_id, confirm = order
                        logging.info(f"[PLC] Fetched from DB: ID={order_id}, CONFIRM={confirm}")

                        if confirm == 1:
                            comm.Write(LABEL_OK_TAG, 1)
                            logging.info(f"[PLC] Sent {LABEL_OK_TAG} = 1")
                        elif confirm == 0:
                            comm.Write(LABEL_NOK_TAG, 1)
                            logging.info(f"[PLC] Sent {LABEL_NOK_TAG} = 1")
                        else:
                            logging.warning(f"[PLC] Unknown CONFIRM value for ID={order_id}")

                        mark_order_acknowledged(order_id)
                    else:
                        logging.info("[PLC] No unconfirmed orders found")

                # Falling edge (1 → 0)
                elif trigger == 0 and previous_trigger == 1:
                    comm.Write(LABEL_OK_TAG, 0)
                    comm.Write(LABEL_NOK_TAG, 0)
                    logging.info("[PLC] Falling edge detected — reset OK/NOK tags")

                previous_trigger = trigger
                time.sleep(0.2)

            except Exception as e:
                logging.error(f"[EXCEPTION] Error in PLC loop: {e}")
                traceback.print_exc()
                time.sleep(2)


def main():
    while True:
        try:
            run_plc_service()
        except Exception as e:
            logging.critical(f"[RESTART] Critical exception: {e}. Restarting service in 5 seconds...")
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    main()
