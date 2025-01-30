# Overview
A repo experimenting with event driven architecture using Kafka and Flink consisting of 2 Python applications; 

- A simple Kafka producer-consumer setup for processing weather data. 
- A real-time stock market analytics platform built with Apache Flink and Kafka, featuring automated price monitoring and Slack alert generation. Visualisation for your selected stock ticker via Grafana with InfluxDB.

It is highly recommended that these run in Docker using the docker-compose files provided.

## Weather Application

- **Producer**: Fetches weather data and publishes to Kafka topic
- **Consumer**: Subscribes to Kafka topic and processes weather data
- **Kafka**: Message broker running in Docker

## Stock Application

- **Fully Containerised**: Easily deployable with Docker containers
- **Visualise Your Data**: Visual stock history supported with Grafana integration
- **Real-time Processing**: Continuous processing of stock data from Kafka streams
- **Price Change Detection**: Monitors percentage-based price movements
- **Target Price Alerts**: Notifications when stocks hit specified target prices
- **Slack Integration**: Automatic alerts sent to configured Slack channels
- **Stateful Processing**: Maintains state for price tracking across the stream
- **Fault Tolerance**: Uses Flink's checkpoint and savepoint mechanisms and Kafka offset management
- **Exactly Once Delivery**
- **Graceful Shutdown Handling**

## Prerequisites

- Python 3.x
- Docker and Docker Compose
- Environment variables configured in .env file (see Configuration section)
- Apache Kafka
- Apache Flink
- Slack Workspace (for alerts)

### Data Flow
1. Stock data ingestion through Kafka
2. Kafka Producer to InfluxDB with data visualisation via Grafana
3. Real-time processing via Flink
4. Alert generation for configured conditions
5. Alert delivery to Slack through dedicated Kafka topic

## Configuration

Project variables can be configured in the`.env` file in the src/ dir. 

Note: For the moment certain parameters are still passed as command line arguments to the .py files and can be set in the docker-compose.yml files.

The Stock tracker requires an API key from https://api.polygon.io/v2/aggs/ to run. This should be added to the .env file.

Slack notifications require a Slack App to be configured and a token and channel to be provided in the .env  file. Note: the Slack App will need to be added to your chosen channel once configured. This can be done by typing `@App-Name` and then adding the App in the desired channel.

## Running

Projects can be launched via Docker by pointing at the correct docker-compose file e.g. `docker-compose -f docker-compose.stocks.yml up -d`

The weather producer and consumer .py files can also be launched via command line if you want to run them locally.

To view the command line args run `python producer.py --help`

## Monitoring
**Flink Dashboard**
Access the Flink UI at localhost:8081
Monitor job status, metrics and logs

**Grafana Dashboard**
Access the Grafana UI at localhost:3000
View the stock price history

**InfluxDB**
Access the InfluxDB UI at localhost:8086
Explore InfluxDB data

## Shutdown

To stop the services and cleanup containers run `docker-compose -f docker-compose.stocks.yml down`

