FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8

WORKDIR /workspace

RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    psycopg2-binary

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
