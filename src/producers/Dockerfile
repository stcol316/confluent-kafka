# Use Python as the base image
FROM python:3.13-slim

# Set working directory inside the container
WORKDIR /app

# Ensure docker logs aren't buffered for live logging
ENV PYTHONUNBUFFERED=1

# Copy the producer script into the container
# The root dir is src/
COPY ./producers/producer.py .
COPY ./producers/requirements.txt /tmp/
COPY .env .

# Install dependencies (e.g., confluent-kafka for Kafka producer)
RUN pip install -r /tmp/requirements.txt

# Command to run the producer
CMD ["python", "producer.py"]
