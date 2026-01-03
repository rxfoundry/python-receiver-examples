import logging
import os
import json

from google.cloud import pubsub_v1
from rxfoundry.clients.swifty_api import Prescription

logger = logging.getLogger()

def message_callback(message):
    logger.info(f"Received message: {message.data}")
    try:
        message_json = json.loads(message.data.decode("utf-8"))

        if message_json["message_type"] in ["RX_AVAILABLE", "RX_UPDATE"]:
            prescription = Prescription.from_dict(message_json["message_object"])
            logger.info(f"Processing prescription: {prescription.rx_number}")

            # Do something with the prescription here...

        message.ack()
    except ValueError:
        logger.error("Error decoding message data")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        message.nack()


def message_fetcher():
    logger.info("Starting message fetcher")
    project_id = os.environ.get("GOOGLE_PROJECT_NAME")

    subscription_name = os.environ.get("PUBSUB_SUBSCRIPTION")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=message_callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    message_fetcher()


