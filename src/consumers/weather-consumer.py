#!/usr/bin/env python
 
import sys
from confluent_kafka import Consumer, Producer
import click
import time
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
consumer_config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'broker:29092',

    # Fixed properties
    'group.id':          'weather-data-consumers',
    'auto.offset.reset': 'earliest'
}

# # TODO: This should be set using environment variables
# # Configure Producer
producer_config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": "broker:29092",
    # Fixed properties
    "acks": "all",
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)
IGNORE_LIST = ["time", "interval"]
POLL_INTERVAL = 10

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
        logger.error(f"Error subscribing to topic: {topic} \n Error: {e}")
    
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
                logger.error(f"Merrsage returned with error: {msg.error()}")
            else:
                logger.debug(f"Debug: Message is message")
                logger.debug(f"{msg.value()}")
                route_data(json.loads(msg.value()))
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.debug(f"Debug: Keyboard interrupt")
        pass
    except Exception as err:
        logger.debug(f"Debug: Exception")
        logger.error(f"{err}")
    finally:
        # Leave group and commit final offsets
        consumer.close()

def route_data(data):
    logger.info(f"Routing data: {data['current']}")
    
    # Separate the relevant returned data into individual topics
    for key, value in data["current"].items():
        if key in IGNORE_LIST:
            continue
        else:
            queue_data(key, str(value))
            
def callback(err, event):    
    if err:
        logger.error(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        logger.info(f'{val} sent to {event.topic()} on partition {event.partition()}.')
              
def queue_data(topic, data):
    # Topic will be automatically created if it does not exist
    logger.info(f"Sending data to topic: {topic}")
    producer.produce(topic, value=data, on_delivery=callback)
    producer.flush()
     
if __name__ == '__main__':
    poll_data()
