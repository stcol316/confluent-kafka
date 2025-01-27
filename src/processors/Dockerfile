# Use Python as the base image
# FROM python:3.11-slim

FROM flink:latest

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip install apache-flink

# Create checkpoint directory with proper permissions
RUN mkdir -p /opt/flink/checkpoints && \
    chown -R flink:flink /opt/flink/checkpoints && \
    chmod 777 /opt/flink/checkpoints

# Create savepoint directory with proper permissions
RUN mkdir -p /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/savepoints && \
    chmod 777 /opt/flink/savepoints

# Set working directory inside the container
WORKDIR /app

# Ensure docker logs aren't buffered for live logging
ENV PYTHONUNBUFFERED=1

# Copy the producer script into the container
# The root dir is src/
COPY ./processors/stock_processor.py .
COPY ./processors/requirements.txt /tmp/
COPY ./processors/flink-sql-kafka-connector.jar .
COPY .env .

# Install dependencies
RUN pip install -r /tmp/requirements.txt

# Command to run the processor
CMD ["python", "stock_processor.py"]
