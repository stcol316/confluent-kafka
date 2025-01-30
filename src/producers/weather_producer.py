#!/usr/bin/env python
# Ensure docker is running
# colima start
# confluent local kafka start

import requests
from confluent_kafka import Producer, KafkaException
import click
import time
import json
import sys
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Configure Producer
config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",
    # Fixed properties
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 1000,
    "delivery.timeout.ms": 30000,
    "message.send.max.retries": 3,
}

producer = Producer(config)
MAX_RETRIES = 10


# Fetch some data
# Click sets command line params and their defaults
@click.command()
@click.option(
    "--url",
    type=str,
    default="https://api.open-meteo.com/v1/forecast",
    help="A url for a data source",
)
@click.option(
    "--topic", type=str, default="weather_data", help="The topic to produce the data to"
)
@click.option(
    "--lat",
    type=float,
    default=54.51,
    help="API call parameters as json string",
)
@click.option(
    "--long",
    type=float,
    default=-6.04,
    help="API call parameters as json string",
)
@click.option(
    "--params",
    type=str,
    default='["temperature_2m"]',
    help="API call parameters as json string",
)
def main(url, topic, lat, long, params):
    timespan = os.environ.get("TIMESPAN", "current")
    logger.info(
        f"URL: {url}\nTopic: {topic}\nLatitude: {lat}\nLongitude: {long}\nParams: {params}\nTime: {timespan}"
    )

    # load params to be usable by requests
    paramStr = f'{{"latitude": {lat}, "longitude": {long}, "{timespan}": {params}}}'
    logger.debug(f"Param String: {paramStr}")

    p = deserialize_data(paramStr)
    if p:
        current_retries = 0
        try:
            while True:
                response = requests.get(url, p)

                # If we get a successful response send the data to kafka
                if response.status_code == 200:
                    logger.debug(f"200 Response: {response.json()}")
                    data = serialize_data(response.json())
                    if data:
                        logger.debug(f"Queuing data: {data}")
                        if not queue_data(data, topic):
                            logger.error("Failed to queue data, stopping producer")
                            break
                        # Reset retry counter
                        current_retries = 0
                    else:
                        current_retries += 1
                        if current_retries > MAX_RETRIES:
                            logger.error("Maximum retries exceeded. Exiting")
                            break
                else:
                    logger.error(f"Error fetching data: {response}")
                    current_retries += 1

                    if current_retries > MAX_RETRIES:
                        logger.error("Maximum retries exceeded. Exiting")
                        break
                # Sleep before fetching the data again
                time.sleep(int(os.environ.get("POLL_INTERVAL", 10)))
        except Exception as e:
            logger.error(f"Unhandled error: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            remaining = producer.flush(timeout=30)
            if remaining > 0:
                logger.info(f"{remaining} messages not delivered")
    else:
        logger.error("Unexpected Shutdown")


def deserialize_data(data_json):
    try:
        data = json.loads(data_json)
        return data
    except Exception as err:
        logger.error(f"Error deserializing object: {err}")
        return None


def serialize_data(data):
    try:
        data_json = json.dumps(data)
        return data_json
    except Exception as err:
        logger.error(f"Error serializing message: {err}")
        return None


def delivery_callback(err, event):
    if err:
        # If err is retryable we raise Kafka exception in queue_data()
        if err.retriable():
            raise KafkaException(err)
        else:
            # If non-retryable we try to produce to dead letter queue
            logger.error(f"Produce to topic {event.topic()} failed with error: {err}")
            try:
                producer.produce("dlq", key=event.key(), value=event.value())
            except Exception as e:
                logger.error(f"Failed to send to DLQ: {e}")
    else:
        val = event.value().decode("utf8")
        logger.info(f"{val} sent to {event.topic()} on partition {event.partition()}.")


def queue_data(data, topic):
    # Topic will be automatically created if it does not exist
    logger.info(f"Sending data to topic: {topic}")

    current_retries = 0
    while current_retries < MAX_RETRIES:
        try:
            producer.produce(topic, value=data, on_delivery=delivery_callback)
            return True
        except KafkaException as ke:
            logger.error(f"Kafka Exception occurred: {ke}")
            error = ke.args[0]
            if error.retriable():
                logger.error(f"Retryable producer error: {error}")
                current_retries += 1
            else:
                logger.error(f"Non-retryable producer error: {error}")
                return False
        except BufferError:
            # Flush messages
            logger.info("Producer queue full, waiting for space...")
            producer.poll(1)
        except Exception as e:
            logger.error(f"Unexpected error while producing: {e}")
            return False


if __name__ == "__main__":
    logger.debug("Entered main")
    main()
