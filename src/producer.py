#!/usr/bin/env python
#Ensure docker is running
#colima start
#confluent local kafka start

import requests
from confluent_kafka import Producer
import click
import time
import json
import sys

#Configure Producer
config = {
        # User-specific properties that you must set
        # Port can be found as Plaintext Ports after running confluent local kafka start
        'bootstrap.servers': 'localhost:51729',
        # Fixed properties
        'acks': 'all',
    }

producer = Producer(config)

# Fetch some data
# Click sets command line params and their defaults
@click.command()
@click.option('--url', type=str, default="https://api.open-meteo.com/v1/forecast", help='A url for a data source')
@click.option('--topic', type=str, default="weather_data", help='The topic to produce the data to')
@click.option('--params', type=str, default='{"latitude":54.51,"longitude": -6.04,"hourly": "temperature_2m"}', help='API call parameters as json string')
def fetch_data(url, topic, params):
    
    print(f"URL: {url}\nTopic: {topic}\nParams: {params}\n")
    
    # load params to be usable by requests
    p = json.loads(params)
    
    while True:
        response = requests.get(url, p)

        # If we get a successful response send the data to kafka
        if response.status_code == 200:
            print(response.json())
            queue_data(response.json(), topic)
        else:
            print(f"Error fetching data: {response.status_code}")
            sys.exit(1)
        
        time.sleep(10)

def queue_data(data, topic):
    data_json = json.dumps(data)
    
    # Topic will be automatically created if it does not exist
    producer.produce(topic, value=data_json)
    producer.flush()
    
  
if __name__ == '__main__':    
    fetch_data()
