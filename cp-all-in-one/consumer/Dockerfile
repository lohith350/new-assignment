# Use a base Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/src/app

# Copy the requirements.txt file
COPY requirements.txt .

# Install the necessary Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Kafka consumer script into the container
COPY consumer.py .

# Command to run the Kafka consumer
CMD ["python", "consumer.py"]