# Use Python 3.8 as the base image
FROM python:3.8-slim

# Set working directory inside the container
WORKDIR /app

# Ensure docker logs aren't buffered for live logging
ENV PYTHONUNBUFFERED=1

# Copy the consumer script into the container
COPY consumer.py .
COPY requirements.txt /tmp/

# Install dependencies from our requirements file
RUN pip install -r /tmp/requirements.txt

# Command to run the consumer
CMD ["python", "consumer.py"]
