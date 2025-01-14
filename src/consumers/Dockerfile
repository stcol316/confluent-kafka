# Use Python as the base image
FROM python:3.13-slim

# Set working directory inside the container
WORKDIR /app

# Ensure docker logs aren't buffered for live logging
ENV PYTHONUNBUFFERED=1

# Copy the consumer script into the container
# The root dir is src/
COPY ./consumers/consumer.py .
COPY ./consumers/weather-consumer.py .
COPY ./consumers/requirements.txt /tmp/
COPY .env .

# Install dependencies from our requirements file
RUN pip install -r /tmp/requirements.txt

# Command to run the consumer
CMD ["python", "consumer.py"]
