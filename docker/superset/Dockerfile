FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir \
    "psycopg2-binary==2.9.9" \
    "trino[sqlalchemy]==0.330.0"
USER superset
