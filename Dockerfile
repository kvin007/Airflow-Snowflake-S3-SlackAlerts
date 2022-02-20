FROM apache/airflow:2.2.3-python3.9

# Airflow
ARG AIRFLOW_USER_HOME=/opt/airflow

USER airflow

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt --user

ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_USER_HOME}"

EXPOSE 8080 5555 8793

WORKDIR ${AIRFLOW_USER_HOME}
