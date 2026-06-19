#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python3 --version | cut -d' ' -f2 | cut -d. -f1-2)"
PYTHON_MAJOR="$(echo "$PYTHON_VERSION" | cut -d. -f1)"
PYTHON_MINOR="$(echo "$PYTHON_VERSION" | cut -d. -f2)"

echo "=== Airflow Tutorial Setup ==="
echo "Python version: $PYTHON_VERSION"

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Check constraints; fall back to latest compatible Airflow if unavailable
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

if curl --output /dev/null --silent --head --fail "${CONSTRAINT_URL}"; then
    echo "Airflow version: $AIRFLOW_VERSION"
    echo "Using constraints: ${CONSTRAINT_URL}"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
else
    echo "WARNING: No constraints for Airflow ${AIRFLOW_VERSION} on Python ${PYTHON_VERSION}."
    echo "Installing latest Airflow compatible with Python ${PYTHON_VERSION}..."
    pip install apache-airflow
fi

# Set AIRFLOW_HOME to this directory
export AIRFLOW_HOME="${PWD}/airflow_home"
mkdir -p "${AIRFLOW_HOME}/dags"

# Symlink tutorial DAGs into Airflow's DAG folder
ln -sfn "${PWD}/dags" "${AIRFLOW_HOME}/dags/tutorial"

# Initialize the metadata database
airflow db migrate

echo ""
echo "=== Setup complete ==="
echo "Activate venv:  source venv/bin/activate"
echo "Start webserver: airflow api-server --port 8080"
echo "Start scheduler: airflow scheduler"
echo "Airflow home:    ${AIRFLOW_HOME}"
echo "DAGs location:   ${AIRFLOW_HOME}/dags/tutorial"
