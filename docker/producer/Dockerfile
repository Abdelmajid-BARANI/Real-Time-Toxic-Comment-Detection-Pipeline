FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0

COPY producer.py /app/

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC_RAW=data.raw.stream
ENV POLLING_INTERVAL=30
ENV PRODUCER_MODE=test

CMD ["python", "-u", "producer.py"]
