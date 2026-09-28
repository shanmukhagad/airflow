#!/bin/bash

# Project setup
AIRFLOW_PROJ_DIR="$HOME/Documents/gitlab/airflow"

# Export Airflow Environment Variables
# export AIRFLOW_HOME="$HOME/airflow"
# export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost/airflow"
# export AIRFLOW__CORE__EXECUTOR="LocalExecutor"

# Navigate to project and activate virtual environment
cd "$AIRFLOW_PROJ_DIR" || { echo "Directory not found!"; exit 1; }
source .venv/bin/activate

# Handle graceful exit on Ctrl+C (stops all sub-processes)
cleanup() {
    echo ""
    echo "Stopping all Airflow processes..."
    kill $(jobs -p) 2>/dev/null
    exit 0
}
trap cleanup EXIT INT TERM

echo "Starting Airflow 3 services..."

# Start services in the background
airflow api-server --port 8080 &
airflow scheduler &
airflow dag-processor &
airflow triggerer &

# Wait 4 seconds for API server initialization before opening browser
sleep 4
echo "Opening UI at http://localhost:8080..."
xdg-open http://localhost:8080 >/dev/null 2>&1 &

# Keep script attached to terminal
wait
