FROM apache/airflow:2.9.1-python3.11

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags /opt/airflow/dags

# Copy admin script with correct permissions
COPY --chmod=755 create_admin.sh /create_admin.sh

EXPOSE 8080

# Persist database and create admin always before Airflow starts
CMD ["/bin/bash", "-c", "export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/database/airflow.db && /create_admin.sh && airflow standalone"]
