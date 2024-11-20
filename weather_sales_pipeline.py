from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd

def fetch_weather_data():
    API_KEY = "YOUR_API_KEY"
    city = "Jakarta"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    # Simpan data ke data lake
    # Simpan ke S3 atau Cloud Storage

def fetch_sales_data():
    # Query ke database penjualan untuk ambil data
    pass

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('weather_sales_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    weather_task = PythonOperator(task_id='fetch_weather', python_callable=fetch_weather_data)
    sales_task = PythonOperator(task_id='fetch_sales', python_callable=fetch_sales_data)

    weather_task >> sales_task
