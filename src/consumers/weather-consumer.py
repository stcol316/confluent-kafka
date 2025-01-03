#!/usr/bin/env python
 
import sys
from confluent_kafka import Consumer, Producer
import click
import time
from datetime import datetime
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# TODO: This should be set using environment variables
# Configure Consumer
config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'broker:29092',

    # Fixed properties
    'group.id':          'weather-data-consumers',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(config)

# # TODO: This should be set using environment variables
# # Configure Producer
config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": "broker:29092",
    # Fixed properties
    "acks": "all",
}

producer = Producer(config)

# Click sets command line params and their defaults
@click.command()
@click.option(
    "--topic",
    type=str,
    default="weather_data",
    help="The topic to poll for data",
)
def poll_data(topic):
    # Subscribe to topic
    try:
        consumer.subscribe([topic])
    except Exception as e:
        logger.info(f"Error subscribing to topic: {topic} \n Error: {e}")
    
     # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(0)
            logger.debug(f"Debug: After poll")
            if msg is None:
                logger.debug(f"Debug: Message is None")
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                logger.info(f"Waiting...")
            elif msg.error():
                logger.debug(f"Debug: Message is error")
                logger.info(f"ERROR: {msg.error()}")
            else:
                logger.debug(f"Debug: Message is message")
                # logger.info(f"{datetime.now()} {msg.value()}")
                route_data(json.loads(msg.value()))
            time.sleep(10)
    except KeyboardInterrupt:
        logger.debug(f"Debug: Keyboard interrupt")
        pass
    except Exception as e:
        logger.debug(f"Debug: Exception")
        logger.info(f"{datetime.now()} Error: {e}")
    finally:
        # Leave group and commit final offsets
        consumer.close()

def route_data(data):
    logger.info(f"Routing data: {data['current']}")
    
    for key, value in data["current"].items():
        if key == "time" or key == "interval":
            continue
        else:
            queue_data(key, str(value))
            
def queue_data(topic, data):
    logger.info(f"Sending data: {data} to topic: {topic}")
    # Topic will be automatically created if it does not exist
    producer.produce(topic, value=data)
    producer.flush()
     
if __name__ == '__main__':
    poll_data()
