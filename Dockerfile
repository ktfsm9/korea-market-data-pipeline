FROM apache/airflow:2.8.1-python3.11

USER airflow

COPY requirements-airflow.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
