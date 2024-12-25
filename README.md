# Census Data Pipeline with Apache Airflow

## Overview
This project demonstrates an ETL pipeline using Apache Airflow. The pipeline:
1. Downloads census data.
2. Transforms and validates it.
3. Loads the processed data into a PostgreSQL database.

## Project Structure

airflow_project/
├── dags/                    # DAGs scripts
│   ├── census_pipeline.py   # Main DAG script
├── data/                    # Data files
│   ├── city_census.csv
│   └── filtered_census.csv
├── .gitignore               # Git ignore rules
├── airflow.cfg              # Airflow configuration
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation


## Prerequisites
- **OS**: Ubuntu (WSL)
- **Python**: 3.9+
- **Airflow**: 2.x
- **PostgreSQL**

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone https://github.com/kostas696/airflow-census-pipeline.git
   cd airflow-census-pipeline

2. Setup a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt

3. Initialize Airflow:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

4. Run the scheduler and webserver:
   ```bash
   airflow scheduler &
   airflow webserver &

5. Access Airflow UI at http://localhost:8080 and trigger the DAG.

## License
This project is licensed under the MIT License.


