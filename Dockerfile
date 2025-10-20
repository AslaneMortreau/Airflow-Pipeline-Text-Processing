FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
        python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file (sans apache-airflow qui est déjà installé)
COPY requirements.txt /requirements.txt

# Install Python dependencies (sans apache-airflow)
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom configuration
COPY config/airflow.cfg /opt/airflow/airflow.cfg

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__WEBSERVER__RBAC=True

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copy project code into image (including dags, plugins, utils)
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY utils/ /opt/airflow/plugins/utils/
COPY data/ /opt/airflow/data/

# Set proper permissions
USER root
RUN chown -R airflow:root /opt/airflow
USER airflow
