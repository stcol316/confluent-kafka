# Use Python 3.8 as the base image
FROM python:3.8-slim

# Set working directory inside the container
WORKDIR /app

# Ensure docker logs aren't buffered for live logging
ENV PYTHONUNBUFFERED=1

# Copy the producer script into the container
COPY producer.py .
COPY requirements.txt /tmp/

# Install dependencies (e.g., confluent-kafka for Kafka producer)
RUN pip install -r /tmp/requirements.txt

# Command to run the producer
CMD ["python", "producer.py"]
