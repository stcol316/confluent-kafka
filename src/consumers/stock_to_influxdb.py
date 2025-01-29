#!/usr/bin/env python

import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
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
influx_client=None
MAX_RETRIES = 10

class NoneError(Exception):
    pass

# Click sets command line params and their defaults
def poll_data(topic):

    try:
        consumer = create_consumer()
    except Exception as e:
        logger.error(f"Could not create consumer: {e}")

    try:
        influx_client = create_influx_client()
        if influx_client is None:
            raise NoneError("InfluxDB client not created")

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    except NoneError as ne:
        logger.error(f"NoneError: {ne}")
        return
    except Exception as e:
        logger.error(f"Could not create influxDB client: {e}")
        return
    
    # Subscribe to topic
    try:
        consumer.subscribe([topic])
    except KafkaException as ke:
        logger.error(f"Kafka subscription error: {ke}")
        return
    except Exception as e:
        logger.error(f"Error subscribing to topic: {topic} \n Error: {e}")

    current_retries = 0
    # Poll for new messages from Kafka and print them.
    try:
        while True:
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
                        if current_retries > MAX_RETRIES:
                            logger.error("Maximum retries exceeded. Exiting")
                            break
                        continue
                    else:
                        if event.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            logger.info(f"Subscribed topic not available: {topic}")
                            current_retries += 1
                            time.sleep(int(os.environ.get("POLL_INTERVAL", 10)))
                            continue
                        logger.error(f"Fatal error encountered: {event.error()}")
                        continue

                # Logic to Process valid message here
                logger.debug("Debug: Event is valid")
                logger.debug(f"{event.value()}")

                data = deserialize_data(event.value())
                if data:
                    try:
                        write_to_influx(write_api, data)
                    except Exception as e: 
                        logger.error(f"Error writing to influxDB: {e}")

                current_retries = 0
            except KafkaException as ke:
                logger.error(f"Kafka Exception occurred: {ke}")
                error = ke.args[0]
                if error.retriable():
                    logger.error(f"Retryable error: {error}")
                    current_retries += 1
                    if current_retries > MAX_RETRIES:
                        logger.error("Maximum retries exceeded. Exiting")
                        break
                    continue
                else:
                    logger.error(f"Fatal error: {error}")
                    break
    except KeyboardInterrupt:
        logger.debug("Debug: Keyboard interrupt")
        pass
    except Exception as err:
        logger.debug("Debug: Exception")
        logger.error(f"Unexpected error: {err}")
    finally:
        # Leave group and commit final offsets
        shutdown_consumer()
        shutdown_influx()


def deserialize_data(data_json):
    try:
        data = json.loads(data_json)
        return data
    except Exception as err:
        logger.error(f"Error deserializing object: {err}")
        return None

def write_to_influx(write_api, event):
    if influx_client is not None:
        logger.info(f"Writing to DB: Date: {event['datetime']} Ticker: {event['ticker']} Price: {event['close']}")
        point = Point(f"{os.environ['INFLUX_STOCK_MEASUREMENT']}")\
            .time(f"{event['datetime']}")\
            .tag("ticker", event["ticker"])\
            .field("price", event["close"])
        logger.info(point)
        write_api.write(bucket=os.environ['INFLUXDB_BUCKET'], record=point)

def create_consumer():
    global consumer
    consumer = Consumer(consumer_config)
    return consumer

def create_influx_client():
    global influx_client
    try:
        influx_client = InfluxDBClient(
            url="http://influxdb:8086",
            token=os.environ["INFLUXDB_TOKEN"],
            org=os.environ["INFLUXDB_ORG"]
        )
        influx_client.ping()
        logger.info("Successfully connected to InfluxDB")
        return influx_client
    except KeyError as ke:
        logger.error(f"Missing environment variable: {ke}")
        return None
    except Exception as e:
        logger.error(f"Error creating InfluxDB client: {e}")
        return None

def shutdown_consumer():
    logger.info("Shutting down consumer...")
    try:
        consumer.commit()  # Commit any pending offsets
    except KafkaException as e:
        logger.error(f"Error committing offsets: {e}")
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
    topic = os.environ.get("STOCK_TOPIC", "stock-data")
    poll_data(topic)
