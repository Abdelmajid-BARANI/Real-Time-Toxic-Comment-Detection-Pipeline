FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    numpy==1.24.3 \
    psycopg2-binary==2.9.9

# Copy Spark streaming scripts
COPY stream_normalizer.py /app/
COPY feature_builder.py /app/
COPY model_serving.py /app/

# Copy ML model and tokenizer
COPY toxic_model.h5 /app/
COPY tokenizer.pkl /app/

# Create checkpoint directories
RUN mkdir -p /app/checkpoints/normalizer \
    /app/checkpoints/feature_builder \
    /app/checkpoints/model_serving

WORKDIR /app

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=toxic_coments_db
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=majid2020
