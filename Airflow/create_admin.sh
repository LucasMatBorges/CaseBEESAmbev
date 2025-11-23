#!/bin/bash

airflow db upgrade

# Try to create user (ignores if exists)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Force reset password if user already exists
airflow users reset-password --username admin --password admin || true

exec "$@"
