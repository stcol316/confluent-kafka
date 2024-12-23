#!/usr/bin/env python
 
from confluent_kafka import Consumer

config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'localhost:51729',

    # Fixed properties
    'group.id':          'kafka-python-getting-started',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(config)

def poll_data():
    # Subscribe to topic
    topic = "weather_data"
    consumer.subscribe([topic])
    
     # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                # Extract the (optional) key and value, and print.
                print(msg.value())
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        
if __name__ == '__main__':
    poll_data()
