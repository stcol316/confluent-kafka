#!/usr/bin/env python
 
import sys
from confluent_kafka import Consumer
import click
import time
import logging

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
            if msg is None:
                logger.debug(f"Debug: Message is None")
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                logger.info(f"Waiting...")
            elif msg.error():
                logger.debug(f"Debug: Message is error")
                logger.error(f"Message returned with error: {msg.error()}")
            else:
                logger.debug(f"Debug: Message is message")
                logger.info(f"{msg.value()}")
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
        
if __name__ == '__main__':
    logger.debug("Entered main")
    poll_data()
