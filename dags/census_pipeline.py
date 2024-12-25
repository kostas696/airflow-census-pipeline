from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'census_data_pipeline',
    default_args=default_args,
    description='An ETL pipeline for census data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def download_data(**kwargs):
    url = 'https://raw.githubusercontent.com/practical-bootcamp/week4-assignment1-template/main/city_census.csv'
    df = pd.read_csv(url)
    df.to_csv('/home/kostas696/airflow/data/city_census.csv', index=False)
    kwargs['ti'].log.info("Data downloaded and saved at /home/kostas696/airflow/data/city_census.csv")

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

def transform_data(**kwargs):
    df = pd.read_csv('/home/kostas696/airflow/data/city_census.csv')

    # Transformations
    state_age_medians = df.groupby(['state', 'age'])['weight'].median()

    def impute_weight(row):
        if pd.isnull(row['weight']):
            return state_age_medians.get((row['state'], row['age']), df['weight'].median())
        return row['weight']

    df['weight'] = df.apply(impute_weight, axis=1)
    filtered_df = df[(df['age'] > 30) & (df['state'] == 'Iowa')]

    filtered_df.to_csv('/home/kostas696/airflow/data/filtered_census.csv', index=False)
    kwargs['ti'].log.info("Data transformed and saved at /home/kostas696/airflow/data/filtered_census.csv")

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

def load_data_to_db(**kwargs):
    from sqlalchemy import create_engine
    import pandas as pd

    # Database connection
    engine = create_engine('postgresql+psycopg2://airflow_user:*************@localhost/airflow_db')

    try:
        # Load data from CSV
        df = pd.read_csv('/home/kostas696/airflow/data/filtered_census.csv')

        # Use an explicit connection
        with engine.connect() as connection:
            df.to_sql(
                name='census_data',
                con=connection,
                schema='public',
                if_exists='replace',
                index=False,
                method='multi'
            )
        kwargs['ti'].log.info("Data loaded to PostgreSQL database successfully.")

    except Exception as e:
        kwargs['ti'].log.error(f"An error occurred while loading data to PostgreSQL: {e}")
        raise

load_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data_to_db,
    dag=dag,
)

def validate_data(**kwargs):
    df = pd.read_csv('/home/kostas696/airflow/data/filtered_census.csv')
    if df.empty:
        raise ValueError("No rows available after transformation!")
    kwargs['ti'].log.info(f"Validation passed. Rows in dataset: {len(df)}")

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

def calculate_statistics(**kwargs):
    df = pd.read_csv('/home/kostas696/airflow/data/filtered_census.csv')
    stats = {
        'mean_age': df['age'].mean(),
        'mean_weight': df['weight'].mean(),
        'total_rows': len(df),
    }
    kwargs['ti'].log.info(f"Statistics: {stats}")

statistics_task = PythonOperator(
    task_id='calculate_statistics',
    python_callable=calculate_statistics,
    dag=dag,
)

download_task >> transform_task >> validate_task >> load_task >> statistics_task
