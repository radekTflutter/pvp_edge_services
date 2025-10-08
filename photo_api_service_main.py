import os
import sys
import io
import time
import logging
import requests
import urllib3
import psycopg2
 
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="cp1250", errors="replace")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.db import get_db_connection

PHOTO_API_URL = "https://pvp-local-api-test/api/photos/upload"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [photo_api_service] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def send_image(order_id: int, blob: bytes | None, photo_type: str) -> bool:
    """
    Sends one image to the API.
    """
    if not blob:
        logging.info(f"[SKIP] No image {photo_type} for id={order_id} – skipping")
        return True  # treat as success, do not block

    file_name = f"{photo_type.lower()}.jpg"
    try:
        logging.info(f"[SEND] Sending '{file_name}' (type={photo_type}) "
                     f"for id={order_id} → {PHOTO_API_URL}")

        files = {
            "photo": (file_name, blob, "image/jpeg")
        }
        data = {
            "pvpEdgeHandlingUnitId": str(order_id),
            "photoType": photo_type
        }

        resp = requests.post(PHOTO_API_URL, files=files, data=data,
                             timeout=15, verify=False)

        ok = False
        try:
            j = resp.json()
            ok = bool(j.get("ok") is True or j.get("ok") == "true")
        except Exception:
            j = {}

        if resp.status_code == 200 and ok:
            logging.info(f"[OK] '{file_name}' (type={photo_type}) "
                         f"for id={order_id} sent successfully")
            return True
        else:
            logging.warning(
                f"[FAIL] '{file_name}' (type={photo_type}) "
                f"for id={order_id} → http={resp.status_code}, "
                f"ok={j.get('ok')}, body={resp.text[:200]}"
            )
            return False

    except Exception as e:
        logging.error(f"[EXC] Sending '{file_name}' (type={photo_type}) "
                      f"for id={order_id} → exception: {e}")
        return False


def process_unsent_images():
    logging.info("photo_api_service started")
    while True:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        pvpedge_orders_id,
                        pvpedge_orders_image_ftp,
                        pvpedge_orders_image_cam1,
                        pvpedge_orders_image_cam2,
                        pvpedge_orders_image_cam1_wrapped,
                        pvpedge_orders_image_cam2_wrapped
                    FROM pvpedge_orders
                    WHERE pvpedge_orders_images_blob_saved = TRUE
                      AND pvpedge_orders_images_sent = FALSE
                    ORDER BY pvpedge_orders_id ASC
                    LIMIT 1
                """)
                row = cur.fetchone()

            if not row:
                time.sleep(5)
                continue

            (order_id, img_reader, img_cam1, img_cam2,
             img_wrapped1, img_wrapped2) = row

            logging.info(f"[PROCESS] New image package for id={order_id}")

            ok_reader   = send_image(order_id, img_reader,   "READER")
            ok_cam1     = send_image(order_id, img_cam1,     "CAM_1")
            ok_cam2     = send_image(order_id, img_cam2,     "CAM_2")
            ok_wrap1    = send_image(order_id, img_wrapped1, "WRAPPED_CAM_1")
            ok_wrap2    = send_image(order_id, img_wrapped2, "WRAPPED_CAM_2")

            if ok_reader and ok_cam1 and ok_cam2 and ok_wrap1 and ok_wrap2:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE pvpedge_orders
                        SET pvpedge_orders_images_sent = TRUE
                        WHERE pvpedge_orders_id = %s
                    """, (order_id,))
                conn.commit()
                logging.info(f"[DONE] All images successfully sent for id={order_id}")
            else:
                logging.warning(f"[PARTIAL] Not all images were sent for id={order_id}")

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as db_err:
            logging.error(f"[DB] Database connection error: {db_err}")
            time.sleep(10)
        except Exception as e:
            logging.error(f"[LOOP] Main loop error: {e}")
            time.sleep(10)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    process_unsent_images()
