#!/usr/bin/env python
 
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
import click
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Configure Consumer
config = {
    # User-specific properties that you must set
    'bootstrap.servers': f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",

    # Fixed properties
    'group.id':          f"{os.environ.get('CONSUMER_GROUP', 'weather-data-consumers')}",
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(config)
MAX_RETRIES=10

# Click sets command line params and their defaults
@click.command()
@click.option(
    "--topic",
    type=str,
    default="weather_data",
    help="The topic to poll for data",
)
def main(topic):
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
                msg = consumer.poll(30)
                if msg is None:
                    logger.debug("Debug: Message is None")
                    logger.info("Waiting...")
                    continue
                
                if msg.error():
                    if msg.error().retriable():
                        logger.error(f"Retryable error encountered: {msg.error()}")
                        current_retries +=1
                        if current_retries > MAX_RETRIES: 
                            logger.error("Maximum retries exceeded. Exiting")
                            break
                        continue                     
                    else:
                        if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            logger.info(f"Subscribed topic not available: {topic}")
                            current_retries +=1
                            time.sleep(int(os.environ.get("POLL_INTERVAL", 10)))
                            continue
                        logger.error(f"Fatal error encountered: {msg.error()}")
                        continue
                
                # Logic to Process valid message here
                logger.debug("Debug: Message is valid")
                logger.info(f"{msg.value()}")
                current_retries = 0
            except KafkaException as ke:
                logger.error(f"Kafka Exception occurred: {ke}")
                error = ke.args[0]
                if error.retriable():
                    logger.error(f"Retryable error: {error}")
                    current_retries +=1
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

def shutdown_consumer():
    logger.info("Shutting down consumer...")
    try:
        consumer.commit()  # Commit any pending offsets
    except KafkaException as e:
        logger.error(f"Error committing offsets: {e}")
    finally:
        consumer.close()
        
if __name__ == '__main__':
    logger.debug("Entered main")
    main()
