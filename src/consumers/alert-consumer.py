#!/usr/bin/env python

import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
import time
import logging
from dotenv import load_dotenv
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from confluent_kafka.admin import AdminClient
import json


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

consumer = None
slack_client = None
MAX_RETRIES = 10
SLACK_RATE_LIMIT_EC = 429


class MaxRetriesExceededError(Exception):
    pass


def main():
    try:
        # Verify all necessary env vars are present
        logger.info("Verifying env variables")
        verify_env()

        topic = os.environ["ALERT_TOPIC"]
        broker = f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}"

        # Create Consumer
        logger.info("Creating Consumer")
        create_consumer(broker)

        # Subscribe to topic
        logger.info(f"Subscribing to Topic {topic}")
        subscribe_to_topic(broker, topic)

        logger.info("Creating Slack client")
        get_slack_client()

        # Poll for new messages from Kafka and send alerts to Slack
        logger.info(f"Polling {topic} for events...")
        poll_data(topic)
    except MaxRetriesExceededError as re:
        logger.error(f"Exiting due to too many retries: {re}")
    except KafkaException as ke:
        logger.error(f"Exiting due to Kafka Exception {ke}")
    except KeyboardInterrupt:
        logger.debug("Debug: Keyboard interrupt")
    except Exception as err:
        logger.error(f"Exiting due to unexpected error: {err}")
    finally:
        # Leave group and commit final offsets
        shutdown_consumer()


def poll_data(topic):
    current_retries = 0

    while current_retries < MAX_RETRIES:
        try:
            event = consumer.poll(30)
            if event is None:
                logger.debug("Debug: Message is None")
                logger.info("Waiting...")
                continue

            if event.error():
                if event.error().retriable():
                    logger.error(f"Retryable error encountered: {event.error()}")
                    current_retries += 1
                    continue
                else:
                    if event.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.info(f"Subscribed topic not available: {topic}")
                        current_retries += 1
                        time.sleep(int(os.environ.get("POLL_INTERVAL", 10)))
                        continue
                    logger.error(f"Fatal error encountered: {event.error()}")
                    raise KafkaException(event.error())

            # Event received
            data = deserialize_data(event.value())
            if data:
                try:
                    logger.debug("Sending message to slack")
                    slack_alert(data, slack_client)
                    current_retries = 0
                except MaxRetriesExceededError as re:
                    logger.error(f"Slack Retries exceeded error: {re}")
                    current_retries += 1
                    continue
                except SlackApiError as se:
                    logger.error(f"Slack API error: {se}")
                    current_retries += 1
                    continue
        except KafkaException as ke:
            logger.error(f"Kafka Exception occurred: {ke}")
            error = ke.args[0]
            if error.retriable():
                logger.error(f"Retryable error: {error}")
                current_retries += 1
                continue
            else:
                logger.error(f"Fatal error: {error}")
                raise KafkaException(error)

    raise MaxRetriesExceededError(f"Maximum retries exceeded while polling {topic}.")


def deserialize_data(data):
    try:
        logger.debug("Deserializing message")
        data_json = json.loads(data)
        return data_json
    except Exception as err:
        logger.error(f"Error serializing message: {err}")
        return None


def subscribe_to_topic(broker, topic):
    current_retries = 0
    admin_client = None
    try:
        while current_retries < MAX_RETRIES:
            try:
                admin_client = AdminClient({"bootstrap.servers": broker})
                metadata = admin_client.list_topics(timeout=10)

                # Block until topic is created by the Flink Processor
                while topic not in metadata.topics:
                    logger.info(f"Topic {topic} not yet available. Waiting...")
                    time.sleep(10)
                    metadata = admin_client.list_topics(timeout=10)

                consumer.subscribe([topic])
                logger.info(f"Subscribed to topic {topic}")
                return
            except KafkaException as ke:
                logger.error(f"Kafka subscription error: {ke}")
                if ke.retriable():
                    logger.error(f"Retryable kafka error during subscribe: {ke}")
                    current_retries += 1
                    # We could have a backoff mechanism here
                    time.sleep(1)
                else:
                    logger.error(f"Non-retryable kafka error during subscribe: {ke}")
                    raise
            except Exception as e:
                logger.error(f"Error subscribing to topic: {topic} \n Error: {e}")
                raise
    finally:
        if admin_client:
            admin_client.close()

    raise MaxRetriesExceededError(
        f"Failed to subscribe to topic {topic} after {MAX_RETRIES} attempts"
    )


def create_consumer(broker):
    global consumer

    # Configure Consumer
    consumer_config = {
        # User-specific properties that you must set
        "bootstrap.servers": broker,
        # Fixed properties
        "group.id": f"{os.environ.get('CONSUMER_GROUP', 'stock-data-consumers')}",
        "auto.offset.reset": "earliest",
    }

    current_retries = 0
    while current_retries < MAX_RETRIES:
        try:
            consumer = Consumer(consumer_config)
            return
        except KafkaException as ke:
            logger.error(f"Kafka consumer creation error: {ke}")
            if ke.retriable():
                logger.error(f"Retryable kafka error during consumer creation: {ke}")
                current_retries += 1
                # We could have a backoff mechanism here
                time.sleep(1)
            else:
                logger.error(
                    f"Non-retryable kafka error during consumer creation: {ke}"
                )
                raise
        except Exception as e:
            logger.error(f"Unexpected error while creating consumer: {e}")
            raise

    raise MaxRetriesExceededError(
        f"Failed to create consumer after {MAX_RETRIES} attempts"
    )


def slack_alert(event, slack_client):
    if slack_client is not None:
        message = ""
        current_retries = 0

        if event["alert_type"] == os.environ["TARGET_PRICE_ALERT"]:
            message = handle_tp_alert(event)
        elif event["alert_type"] == os.environ["PERCENTAGE_CHANGE_ALERT"]:
            message = handle_pc_alert(event)
        else:
            logger.info(f"Unexpected or malformed event: {event}")

        logger.info(message)
        if message != "":
            while current_retries < MAX_RETRIES:
                try:
                    response = slack_client.chat_postMessage(
                        channel=os.environ["SLACK_CHANNEL"],
                        text=message,
                    )
                    return
                except SlackApiError as se:
                    if se.response.status_code == SLACK_RATE_LIMIT_EC:
                        # Get Retry-After value
                        delay = int(se.response.headers["Retry-After"])
                        logger.debug(f"Rate limited. Retrying in {delay} seconds")
                        time.sleep(delay)
                    elif se.response["error"] in ["invalid_auth", "channel_not_found"]:
                        logger.error(f"Non-Retryable Error: {se.response['error']}")
                        raise
                    else:
                        logger.error(f"Failed to push message to Slack: {str(se)}")
                        current_retries += 1
                        # We are rate limited to 1 message per second
                        time.sleep(1.2)

            raise MaxRetriesExceededError(
                f"Failed to push message to Slack after {MAX_RETRIES} attempts"
            )


def handle_tp_alert(event):
    message = f"{event['datetime']} {event['ticker']} price has reached the target of {event['target_price']}. Current price: {event['current_price']}"
    return message


def handle_pc_alert(event):
    if event["price_change_percentage"] > 0:
        message = f"{event['datetime']} {event['ticker']} price has increased by {event['price_change_percentage']}%. Current price: {event['current_price']}"
    else:
        message = f"{event['datetime']} {event['ticker']} price has decreased by {event['price_change_percentage']}%. Current price: {event['current_price']}"
    return message


def get_slack_client():
    global slack_client
    slack_token = os.environ.get("SLACK_TOKEN", None)
    if slack_token is not None:
        slack_client = WebClient(token=slack_token)


def verify_env():
    required_vars = [
        "BROKER_NAME",
        "BROKER_LISTENER_PORT",
        "ALERT_TOPIC",
        "TARGET_PRICE_ALERT",
        "PERCENTAGE_CHANGE_ALERT",
    ]
    missing_vars = [var for var in required_vars if var not in os.environ]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


def shutdown_consumer():
    logger.info("Shutting down consumer...")
    current_retries = 0
    try:
        while current_retries < MAX_RETRIES:
            try:
                # Commit any pending offsets, break if success
                consumer.commit()
                break
            except KafkaException as ke:
                logger.error(f"Error committing offsets: {ke}")
                if ke.retriable():
                    logger.error(f"Retryable kafka error during shutdown: {ke}")
                    current_retries += 1
                    # We could have a backoff mechanism here
                    time.sleep(1)
                else:
                    logger.error(f"Non-retryable kafka error during shutdown: {ke}")
                    break
    finally:
        consumer.close()


if __name__ == "__main__":
    logger.debug("Entered main")
    main()
