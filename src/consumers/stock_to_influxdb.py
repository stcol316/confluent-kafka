#!/usr/bin/env python

import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
import json
import time
import logging
from dotenv import load_dotenv
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Configure Consumer
consumer_config = {
    # User-specific properties that you must set
    "bootstrap.servers": f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",
    # Fixed properties
    "group.id": f"{os.environ.get('CONSUMER_GROUP', 'stock-data-consumers')}",
    "auto.offset.reset": "earliest",
}

consumer = None
influx_client = None
MAX_RETRIES = 10


class NoneError(Exception):
    pass


class MaxRetriesExceededError(Exception):
    pass


def main():
    try:
        # Verify all necessary env vars are present
        logger.info("Verifying env variables")
        verify_env()

        topic = os.environ.get("STOCK_TOPIC", "stock-data")
        broker = f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}"

        # Create Consumer
        logger.info("Creating Consumer")
        create_consumer(broker)

        # Create InfluxDB Client
        logger.info("Creating InfluxDB Client")
        create_influx_client()

        if influx_client is None:
            raise NoneError("InfluxDB client not created")

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        # Subscribe to topic
        logger.info(f"Subscribing to Topic {topic}")
        subscribe_to_topic(broker, topic)

        # Poll for new messages from Kafka and send them to Influx
        logger.info(f"Polling {topic} for events...")
        poll_data(topic, write_api)
    except NoneError as ne:
        logger.error(f"Exiting due to NoneError: {ne}")
    except KeyError as ke:
        logger.error(f"Exiting due to KeyError {ke}")
    except KeyboardInterrupt:
        logger.debug("Debug: Keyboard interrupt")
    except Exception as err:
        logger.error(f"Exiting due to unexpected error: {err}")
    finally:
        # Leave group and commit final offsets
        shutdown_consumer()
        shutdown_influx()


def poll_data(topic, write_api):
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
                    logger.debug("Sending data to InfluxDB")
                    write_to_influx(write_api, data)
                    current_retries = 0
                except Exception as e:
                    logger.error(f"Error writing to influxDB: {e}")
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
                error = ke.args[0]
                if error.retriable():
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


def verify_env():
    required_vars = [
        "BROKER_NAME",
        "BROKER_LISTENER_PORT",
        "STOCK_TOPIC",
        "INFLUXDB_TOKEN",
        "INFLUXDB_ORG",
        "INFLUXDB_URL",
        "INFLUXDB_BUCKET",
        "INFLUX_STOCK_MEASUREMENT",
    ]
    missing_vars = [var for var in required_vars if var not in os.environ]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


def deserialize_data(data_json):
    try:
        data = json.loads(data_json)
        return data
    except Exception as err:
        logger.error(f"Error deserializing object: {err}")
        return None


def write_to_influx(write_api, event):
    if influx_client is not None:
        logger.info(
            f"Writing to DB: Date: {event['datetime']} Ticker: {event['ticker']} Price: {event['close']}"
        )
        point = create_point(event)
        logger.debug(point)
        if point:
            try:
                write_api.write(bucket=os.environ["INFLUXDB_BUCKET"], record=point)
            except (ValueError, TypeError) as te:
                logger.error(f"Data type error creating point: {te}")
            except Exception as e:
                logger.error(f"Failed to write to InfluxDB: {e}")


def create_point(event):
    try:
        point = (
            Point(f"{os.environ['INFLUX_STOCK_MEASUREMENT']}")
            .time(f"{event['datetime']}")
            .tag("ticker", event["ticker"])
            .field("price", round(float(event["close"]), 2))
        )
        return point
    except (ValueError, TypeError) as e:
        logger.error(
            f"Error encoutered while creating datapoint: Event: {event} | Error: {e}"
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
            error = ke.args[0]
            if error.retriable():
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


def create_influx_client():
    global influx_client
    try:
        influx_client = InfluxDBClient(
            url=os.environ["INFLUXDB_URL"],
            token=os.environ["INFLUXDB_TOKEN"],
            org=os.environ["INFLUXDB_ORG"],
        )
        influx_client.ping()
        logger.info("Successfully connected to InfluxDB")
    except KeyError as ke:
        logger.error(f"Missing environment variable: {ke}")
        raise
    except Exception as e:
        logger.error(f"Error creating InfluxDB client: {e}")
        raise


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
                error = ke.args[0]
                if error.retriable():
                    logger.error(f"Retryable kafka error during shutdown: {ke}")
                    current_retries += 1
                    # We could have a backoff mechanism here
                    time.sleep(1)
                else:
                    logger.error(f"Non-retryable kafka error during shutdown: {ke}")
                    break
    finally:
        consumer.close()


def shutdown_influx():
    global influx_client
    if influx_client is not None:
        logger.info("Closing InfluxDB client connection...")
        try:
            influx_client.close()
        except Exception as e:
            logger.error(f"Error closing InfluxDB client: {e}")
        finally:
            influx_client = None


if __name__ == "__main__":
    logger.debug("Entered main")
    main()
