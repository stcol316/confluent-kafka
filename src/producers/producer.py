#!/usr/bin/env python
# Ensure docker is running
# colima start
# confluent local kafka start

import requests
from confluent_kafka import Producer
import click
import time
import json
import sys
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Configure Producer
config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": "broker:29092",
    # Fixed properties
    "acks": "all",
}

producer = Producer(config)


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
    default= 54.51,
    help="API call parameters as json string",
)
@click.option(
    "--long",
    type=float,
    default= -6.04,
    help="API call parameters as json string",
)
@click.option(
    "--params",
    type=str,
    default='["temperature_2m"]',
    help="API call parameters as json string",
)
def fetch_data(url, topic, lat, long, params):
    logger.info(f"URL: {url}\nTopic: {topic}\nLatitude: {lat}\nLongitude: {long}\nParams: {params}\n")

    # load params to be usable by requests
    paramStr=(f'{{"latitude": {lat}, "longitude": {long}, "current": {params}}}')
    logger.debug(paramStr)
    p = json.loads(paramStr)

    try:
        while True:
            response = requests.get(url, p)

            # If we get a successful response send the data to kafka
            if response.status_code == 200:
                logger.info(f"{response.json()}")
                queue_data(response.json(), topic)
                # break
            else:
                logger.info(f"Error fetching data: {response.status_code}")
                # sys.exit(1)

            # Sleep before fetching the data again
            time.sleep(20)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


def queue_data(data, topic):
    data_json = json.dumps(data)

    # Topic will be automatically created if it does not exist
    producer.produce(topic, value=data_json)
    producer.flush()


if __name__ == "__main__":
    logger.debug("Entered main")
    fetch_data()