from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import psycopg2
from psycopg2 import sql
import pandas as pd

# Fungsi untuk menyimpan data cuaca ke PostgreSQL
def save_weather_data_to_postgres(data):
    # Koneksi ke PostgreSQL
    conn = psycopg2.connect(
        host="postgres",  # Nama service PostgreSQL di docker-compose
        database="airflow",  # Nama database
        user="airflow",  # User PostgreSQL
        password="airflow"  # Password PostgreSQL
    )
    cur = conn.cursor()

    # Data cuaca yang akan disimpan
    city = data['name']
    temperature = data['main']['temp']
    description = data['weather'][0]['description']

    # Query untuk menyimpan data ke tabel weather_data
    insert_query = sql.SQL("""
        INSERT INTO weather_data (city, temperature, description)
        VALUES (%s, %s, %s)
    """)
    cur.execute(insert_query, (city, temperature, description))

    # Commit perubahan dan tutup koneksi
    conn.commit()
    cur.close()
    conn.close()

# Fungsi untuk mengambil data cuaca dari API
def fetch_weather_data():
    API_KEY = "23ccfcc8589091dc0eaece442396754b"
    city = "Jakarta"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    
    # Simpan data ke PostgreSQL
    save_weather_data_to_postgres(data)

# Fungsi untuk mengambil dan memproses data penjualan dari CSV
def fetch_sales_data():
    # Path file CSV
    file_path = "/D:\my-airflow-project\dags\retail_sales_dataset.csv"
    
    # Membaca file CSV dengan Pandas
    df = pd.read_csv(file_path)

    # Koneksi ke PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Menyimpan setiap baris data ke dalam tabel penjualan
    for index, row in df.iterrows():
        # Menyesuaikan kolom dengan yang ada di CSV
        sales_id = row['sales_id']
        product = row['product']
        quantity = row['quantity']
        price = row['price']

        insert_query = sql.SQL("""
            INSERT INTO sales_data (sales_id, product, quantity, price)
            VALUES (%s, %s, %s, %s)
        """)
        cur.execute(insert_query, (sales_id, product, quantity, price))

    # Commit perubahan dan tutup koneksi
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('weather_sales_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    weather_task = PythonOperator(task_id='fetch_weather', python_callable=fetch_weather_data)
    sales_task = PythonOperator(task_id='fetch_sales', python_callable=fetch_sales_data)

    # Menjalankan tugas secara berurutan: pertama data cuaca, baru penjualan
    weather_task >> sales_task
