import json
import time
import random
import logging
import paho.mqtt.client as mqtt
import os

from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG VARIABLES ----------------
MQTT_HOST = os.getenv("MQTT_HOST")

MQTT_PORT = int(os.getenv("MQTT_PORT"))

MQTT_TOPIC = os.getenv("MQTT_TOPIC")

MQTT_QOS = int(os.getenv("MQTT_QOS"))

# ---------------- LOGGING ----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/publisher.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ---------------- MQTT CALLBACKS ----------------

def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:
        logger.info("Connected to MQTT Broker")
    else:
        logger.info(f"Connection failed with code {reason_code}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    logger.warning("Disconnected from MQTT Broker")


# ---------------- MQTT CLIENT ----------------

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.on_connect = on_connect
client.on_disconnect = on_disconnect

client.connect(MQTT_HOST, MQTT_PORT)

# starts background network thread
client.loop_start()


# ---------------- PUBLISH LOOP ----------------

published_count = 0
failed_count = 0


while True:

    payload = {
        "device_id": random.randint(1, 100),
        "temperature": random.randint(20, 40),
        "timestamp": time.time()
    }
    
    result = client.publish(
        MQTT_TOPIC,
        json.dumps(payload),
        qos=MQTT_QOS
    )

    status = result.rc

    if status == mqtt.MQTT_ERR_SUCCESS:

        published_count += 1

        if published_count % 100 == 0:
            logger.info(f"Published: {published_count}")

    else:

        failed_count += 1

        logger.error(f"Publish Failed | Failed Count: {failed_count}")

    time.sleep(0.01)