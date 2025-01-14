#!/usr/bin/env python
 
import sys
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import click
import time
import logging
import json
from dotenv import load_dotenv
import os

load_dotenv()

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
    'bootstrap.servers': f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",

    # Fixed properties
    'group.id':          'weather-data-consumers',
    'auto.offset.reset': 'earliest'
}

# # TODO: This should be set using environment variables
# # Configure Producer
producer_config = {
    # User-specific properties that you must set
    # Port can be found as Plaintext Ports after running confluent local kafka start
    "bootstrap.servers": f"{os.environ['BROKER_NAME']}:{os.environ['BROKER_LISTENER_PORT']}",
    # Fixed properties
    "acks": "all",
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)
IGNORE_LIST = ["time", "interval"]
MAX_RETRIES = 10

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
    
    current_retries = 0
     # Poll for new messages from Kafka and produce them.
    try:
        timespan = os.environ.get('TIMESPAN', 'current')
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
                        break
                    
                # Process valid message
                logger.debug("Debug: Message is message")
                data = deserialize_data(msg.value())
                if data:
                    route_data(data, timespan)
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
        
def deserialize_data(data):
    try:
        logger.debug("Deserializing message")
        data_json = json.loads(data)
        return data_json
    except Exception as err:
        logger.error(f"Error serializing message: {err}")
        return None
    
def route_data(data, timespan):
    logger.debug(f"Routing data: {data}")
    
    if timespan in data:
        logger.info(f"Routing data: {data[timespan]}")
        
        # Separate the relevant returned data into individual topics
        for key, value in data[timespan].items():
            if key in IGNORE_LIST:
                continue
            else:
                queue_data(key, str(value))
    else:
        logger.error(f"Error: Timespan {timespan} does not exist in data {data}")
            
def delivery_callback(err, event):    
    if err:
    # If err is retryable we raise Kafka exception in queue_data()
        if err.retriable():
            raise KafkaException(err)
        else:
            # If non-retryable we try to produce to dead letter queue
            logger.error(f'Produce to topic {event.topic()} failed with error: {err}')
            try:
                producer.produce('dlq', key=event.key(), value=event.value())
            except Exception as e:
                logger.error(f'Failed to send to DLQ: {e}')
    else:
        val = event.value().decode('utf8')
        logger.info(f'{val} sent to {event.topic()} on partition {event.partition()}.')
              
def queue_data(topic, data):
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
                current_retries +=1
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
     
if __name__ == '__main__':
    poll_data()
