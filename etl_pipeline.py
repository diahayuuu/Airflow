from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# API key OpenWeatherMap
API_KEY = "23ccfcc8589091dc0eaece442396754b"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'etl_weather_and_sales_data',
    default_args=default_args,
    description='ETL pipeline for weather and sales data',
    schedule_interval=timedelta(days=1),
)

# 1. Extract Weather Data from OpenWeatherMap API
def fetch_weather_data(city, ti):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        weather_data = {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'weather': data['weather'][0]['description'],
            'datetime': datetime.now(),
        }
        # Push weather data to XCom
        ti.xcom_push(key='weather_data', value=weather_data)
    else:
        raise Exception(f"Failed to fetch weather data for {city}: {response.text}")

# 2. Extract Sales Data from CSV
def fetch_sales_data(file_path, ti):
    sales_data = pd.read_csv('retail_sales_dataset.csv')
    ti.xcom_push(key='sales_data', value=sales_data.to_dict('records'))

# 3. Transform and Combine Data
def transform_and_combine_data(ti):
    weather_data = ti.xcom_pull(key='weather_data', task_ids='fetch_weather_data')
    sales_data = ti.xcom_pull(key='sales_data', task_ids='fetch_sales_data')
    
    if not weather_data or not sales_data:
        raise ValueError("Missing weather or sales data")
    
    # Add weather information to each sales record
    combined_data = []
    for sale in sales_data:
        sale.update({
            'city': weather_data['city'],
            'temperature': weather_data['temperature'],
            'humidity': weather_data['humidity'],
            'weather': weather_data['weather'],
            'weather_datetime': weather_data['datetime'],
        })
        combined_data.append(sale)
    
    ti.xcom_push(key='combined_data', value=combined_data)

# 4. Load Data into PostgreSQL
def load_data_to_postgres(ti):
    combined_data = ti.xcom_pull(key='combined_data', task_ids='transform_and_combine_data')
    if not combined_data:
        raise ValueError("No combined data to load")
    
    postgres_hook = PostgresHook(postgres_conn_id='etl_connection')
    insert_query = """
    INSERT INTO sales_weather (
        sale_id, product, quantity, price, sale_datetime,
        city, temperature, humidity, weather, weather_datetime
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for record in combined_data:
        postgres_hook.run(insert_query, parameters=(
            record['sale_id'], record['product'], record['quantity'], 
            record['price'], record['sale_datetime'], record['city'], 
            record['temperature'], record['humidity'], record['weather'], 
            record['weather_datetime']
        ))

# Define tasks
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    op_args=['Yogyakarta'],  # Replace with your desired city
    dag=dag,
)

fetch_sales_data_task = PythonOperator(
    task_id='fetch_sales_data',
    python_callable=fetch_sales_data,
    op_args=['retail_sales_dataset.csv'],  # Replace with the actual path to your sales CSV
    dag=dag,
)

transform_and_combine_data_task = PythonOperator(
    task_id='transform_and_combine_data',
    python_callable=transform_and_combine_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='etl_connection',
    sql=f"""
    CREATE TABLE IF NOT EXISTS sales_weather (
        sale_id INT PRIMARY KEY,
        product TEXT,
        quantity INT,
        price FLOAT,
        sale_datetime TIMESTAMP,
        city TEXT,
        temperature FLOAT,
        humidity INT,
        weather TEXT,
        weather_datetime TIMESTAMP
    ) IN DATABASE etl_project;
    """,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# Define task dependencies
[fetch_weather_data_task, fetch_sales_data_task] >> transform_and_combine_data_task >> create_table_task >> load_data_task
