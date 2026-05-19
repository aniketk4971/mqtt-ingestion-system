import json
import threading
import time
import logging
import atexit
import signal
import sys
import os
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from django.core.management.base import BaseCommand
from ingestion.mongo_service import collection

load_dotenv()

# ---------------- CONFIG VARIABLES ----------------
MQTT_HOST = os.getenv("MQTT_HOST")

MQTT_PORT = int(os.getenv("MQTT_PORT"))

MQTT_TOPIC = os.getenv("MQTT_TOPIC")

MQTT_QOS = int(os.getenv("MQTT_QOS"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT"))

# ---------------- LOGGING ----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/consumer.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

mongodb_logger = logging.getLogger("mongodb_logger")

mongodb_handler = logging.FileHandler("../logs/mongodb.log")

mongodb_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(message)s"
    )
)

mongodb_logger.addHandler(mongodb_handler)
mongodb_logger.addHandler(logging.StreamHandler())
mongodb_logger.setLevel(logging.INFO)

# ---------------- BUFFER ----------------

buffer = []

# Thread safety lock
buffer_lock = threading.Lock()

received_count = 0
inserted_count = 0
failed_count = 0


# ---------------- MQTT CALLBACKS ----------------

def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:
        logger.info("Connected to MQTT Broker")
    else:
        logger.error(f"Connection Failed: {reason_code}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):

    logger.warning("Disconnected from MQTT Broker")


def on_message(client, userdata, msg):

    global received_count

    try:

        data = json.loads(msg.payload)

        # Thread-safe buffer append
        with buffer_lock:
            buffer.append(data)

        received_count += 1

        # Reduce log flooding
        if received_count % 100 == 0:
            logger.info(f"Received: {received_count}")

    except Exception as e:

        logger.error(f"Message Processing Error: {e}")


# ---------------- BATCH INSERT WORKER ----------------

def batch_insert_worker():

    global inserted_count
    global failed_count

    last_flush_time = time.time()

    while True:

        batch = []

        try:

            current_time = time.time()

            should_flush = (
                len(buffer) >= BATCH_SIZE
                or (
                    len(buffer) > 0
                    and current_time - last_flush_time >= BATCH_TIMEOUT
                )
            )

            if should_flush:

                # Thread-safe batch extraction
                with buffer_lock:

                    batch = buffer[:]

                    buffer.clear()

                if batch:

                    collection.insert_many(
                        batch,
                        ordered=False
                    )

                    inserted_count += len(batch)

                    last_flush_time = current_time

                    mongodb_logger.info(
                        f"Inserted Batch | Batch Size: {len(batch)} | Total Inserted: {inserted_count}"
                    )

        except Exception as e:

            failed_count += len(batch)

            logger.error(f"Mongo Insert Error: {e}")

        time.sleep(1)


# ---------------- GRACEFUL SHUTDOWN ----------------

def flush_remaining_data():

    global inserted_count

    try:

        with buffer_lock:

            if len(buffer) > 0:

                logger.info(
                    f"Flushing Remaining {len(buffer)} Records Before Shutdown"
                )

                collection.insert_many(
                    buffer,
                    ordered=False
                )

                inserted_count += len(buffer)

                logger.info(
                    f"Successfully Flushed Remaining Records | Total Inserted: {inserted_count}"
                )

                buffer.clear()

    except Exception as e:

        logger.error(f"Flush Error During Shutdown: {e}")


# Register automatic cleanup
atexit.register(flush_remaining_data)


def shutdown_handler(signum, frame):

    logger.warning("Shutdown Signal Received")

    flush_remaining_data()

    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ---------------- DJANGO COMMAND ----------------

class Command(BaseCommand):

    def handle(self, *args, **kwargs):

        # Start batch worker thread
        threading.Thread(
            target=batch_insert_worker,
            daemon=True
        ).start()

        # MQTT client
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2
        )

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message

        client.connect(MQTT_HOST, MQTT_PORT)

        client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)

        logger.info("MQTT Consumer Started")

        client.loop_forever()

