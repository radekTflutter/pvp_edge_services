import time
import logging
import requests
import sys
import os
import io
from datetime import datetime, timezone

# ensure utf-8 stdout (for PVPRunAll GUI)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.db import get_db_connection
import urllib3

# configuration
API_URL = "https://pvp-local-api-test/api/handling-units/save"
REQUEST_TIMEOUT = 12
MAX_HTTP_ATTEMPTS = 4
INITIAL_BACKOFF_SEC = 1.0

# disable insecure warnings (we still use verify=False below as original)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# logger
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [api_service] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("api_service")

def to_zulu_str(ts):
    """Convert a datetime (naive or aware) to 'YYYY-MM-DDTHH:MM:SSZ' (UTC, no ms).
       If ts is None, return current UTC time string."""
    if ts is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not isinstance(ts, datetime):
        # safety: if ts not datetime, fallback to now
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_utc = ts.astimezone(timezone.utc)
    return ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def post_with_retry(session, url, json_payload, max_attempts=MAX_HTTP_ATTEMPTS, initial_backoff=INITIAL_BACKOFF_SEC):
    """POST with simple exponential backoff. Returns (response, response_text) or raises last exception."""
    attempt = 1
    while True:
        try:
            log.info(f"HTTP POST attempt {attempt}/{max_attempts} (payload id={json_payload.get('pvpEdgeId')})")
            resp = session.post(url, json=json_payload, timeout=REQUEST_TIMEOUT, verify=False)
            # Always return response; caller decides if it's success. We'll retry on server errors (5xx) or 429.
            status = resp.status_code
            text_snippet = (resp.text[:500] + '...') if len(resp.text) > 500 else resp.text
            log.info(f"HTTP response status={status}; body_snippet={text_snippet!r}")
            if status >= 500 or status == 429:
                if attempt < max_attempts:
                    backoff = initial_backoff * (2 ** (attempt - 1))
                    log.warning(f"Server error or rate limit (status={status}), retrying after {backoff}s")
                    time.sleep(backoff)
                    attempt += 1
                    continue
            return resp
        except requests.RequestException as e:
            log.error(f"RequestException on attempt {attempt}: {e}")
            if attempt < max_attempts:
                backoff = initial_backoff * (2 ** (attempt - 1))
                log.info(f"Retrying HTTP in {backoff}s")
                time.sleep(backoff)
                attempt += 1
                continue
            # re-raise the last exception to caller
            raise

def fetch_next_order(conn):
    """Return single next order row or None: (id, hu, confirm, timestamp)"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pvpedge_orders_id,
                   pvpedge_orders_hu,
                   pvpedge_orders_confirm,
                   pvpedge_orders_timestamp
            FROM pvpedge_orders
            WHERE api_data_sent = FALSE
              AND pvpedge_orders_confirm IS NOT NULL
            ORDER BY pvpedge_orders_timestamp ASC
            LIMIT 1
        """)
        return cur.fetchone()

def mark_order_sent(conn, order_id):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pvpedge_orders
            SET api_data_sent = TRUE
            WHERE pvpedge_orders_id = %s
        """, (order_id,))
    conn.commit()

def send_one_order():
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            log.error("No DB connection available")
            return

        row = fetch_next_order(conn)
        if not row:
            # nothing to do
            return

        order_id, hu, confirm, ts = row
        scan_timestamp = to_zulu_str(ts)
        payload = {
            "pvpEdgeId": int(order_id),
            "plantCode": "PL02",
            "handlingUnitLabelCode": hu,
            "wrapped": True,
            "wrapperEnabled": True,
            "labelConfirmed": True if int(confirm) == 1 else False,
            "readerEnabled": True,
            "scanTimestamp": scan_timestamp
        }

        log.info(f"Preparing to send order id={order_id}, hu={hu}, confirm={confirm}, scanTimestamp={scan_timestamp}")
        session = requests.Session()
        # set sensible headers
        session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

        try:
            resp = post_with_retry(session, API_URL, payload)
        except Exception as e:
            log.error(f"HTTP POST failed after retries for id={order_id}: {e}")
            return

        # parse response JSON safely
        resp_json = {}
        try:
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype.lower():
                resp_json = resp.json()
        except Exception as e:
            log.warning(f"Failed to parse JSON response for id={order_id}: {e}")

        ok_flag = resp_json.get("ok")
        success = (resp.status_code == 200 and (ok_flag is True or ok_flag == "true"))
        if success:
            log.info(f"API accepted order id={order_id} (HTTP {resp.status_code})")
            try:
                mark_order_sent(conn, order_id)
                log.info(f"Marked order id={order_id} as api_data_sent = TRUE")
            except Exception as e:
                log.error(f"Database update after API success failed for id={order_id}: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
        else:
            # not successful: log details for later manual inspection
            snippet = (resp.text[:1000] + "...") if resp.text and len(resp.text) > 1000 else resp.text
            log.warning(f"API returned non-success for id={order_id}: http={resp.status_code}, ok={ok_flag}, body_snippet={snippet}")

    except Exception as e:
        log.exception(f"Unexpected error in send_one_order: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def main():
    log.info("api_service started")
    while True:
        try:
            send_one_order()
        except Exception as e:
            log.exception(f"Top-level loop error: {e}")
        # small sleep to avoid busy-looping
        time.sleep(1)

if __name__ == "__main__":
    main()
