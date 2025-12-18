FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.29.0 \
    pandas==2.1.3 \
    plotly==5.18.0 \
    psycopg2-binary==2.9.9

COPY dashboard.py /app/
COPY .streamlit /app/.streamlit

ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=toxic_coments_db
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=majid2020

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
