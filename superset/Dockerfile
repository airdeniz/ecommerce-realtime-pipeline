FROM apache/superset:latest

USER root

RUN apt-get update && apt-get install -y gcc g++ libsasl2-dev && \
    pip install --target=/app/.venv/lib/python3.10/site-packages "pyhive[hive]" psycopg2-binary

COPY superset_config.py /app/pythonpath/superset_config.py

USER superset