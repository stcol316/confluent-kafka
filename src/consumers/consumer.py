#!/usr/bin/env python
 
from confluent_kafka import Consumer
import click
import time
from datetime import datetime

config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'broker:29092',

    # Fixed properties
    'group.id':          'weather-data-consumers',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(config)

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
        print(f"{datetime.now()} Error subscribing to topic: {topic} \n Error: {e}")
    
     # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print(f"{datetime.now()} Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                print(f"{datetime.now()} {msg.value()}")
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        
if __name__ == '__main__':
    poll_data()
