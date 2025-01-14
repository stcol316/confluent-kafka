# confluent-kafka
A demonstration project showing a simple producer-consumer setup using Confluent Kafka with Python. The project includes a weather data producer and consumer, orchestrated with Docker Compose.

## Architecture

- **Producer**: Fetches weather data and publishes to Kafka topic
- **Consumer**: Subscribes to Kafka topic and processes weather data
- **Kafka**: Message broker running in Docker

## Prerequisites

- Python 3.x
- Docker and Docker Compose
- Environment variables configured (see Configuration section)

## Configuration

Project variable can be configured in the`.env` file in the src/ dir

Requirements will have to be installed running `pip install -r requirements.txt` in the root dir

## Running

Project can be launched via Docker using `docker-compose up -d`

The prodcer and consumer .py files can also be launched via command line.
To view the command line args run `python producer.py --help`

## Shutdown

To stop the services run `docker-compose down`
