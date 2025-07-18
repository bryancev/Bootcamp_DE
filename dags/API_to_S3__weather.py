from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import io

# Города и координаты
CITIES = {
    "Москва": (55.7558, 37.6173),
    "Санкт-Петербург": (59.9343, 30.3351),
    "Екатеринбург": (56.8389, 60.6057),
    "Казань": (55.7963, 49.1084),
}

# URL API и параметры подключения
API_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Asia/Yekaterinburg"

# Параметры сохранения данных
S3_CONN_ID = "minios3_conn" # Admin - Connections
BUCKET_NAME = "dev"
CATALOG_NAME = "bryantsev"
TABLE_NAME = "open_meteo"

def extract_data(**context):
    """Загрузка сырых погодных данных из API Open-Meteo"""
    ds = context['ds']
    ti = context['ti']
    raw = {}

    for city, (lat, lon) in CITIES.items():
        try:
            resp = requests.get(
                API_URL,
                params={
                    'latitude': lat,
                    'longitude': lon,
                    'hourly': 'temperature_2m,rain',
                    'start_date': ds,
                    'end_date': ds,
                    'timezone': TIMEZONE,
                },
                timeout=15
            )
            resp.raise_for_status()
            raw[city] = resp.json().get('hourly', {})
            ti.log.info(f"Данные получены для {city}")
        except Exception as e:
            ti.log.error(f"Ошибка получения данных для {city}: {e}")
            raw[city] = {}

    filename = f"{CATALOG_NAME}/{TABLE_NAME}/raw/weather_{ds}.json"
    S3Hook(S3_CONN_ID).load_string(
        string_data=json.dumps(raw),
        key=filename,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    ti.xcom_push(key='raw_filename', value=filename)

def transform_data(**context):
    """Преобразование сырых данных в список записей"""
    ti = context['ti']
    raw_filename = ti.xcom_pull(task_ids='extract_task', key='raw_filename')
    raw_data = S3Hook(S3_CONN_ID).read_key(key=raw_filename, bucket_name=BUCKET_NAME)
    raw = json.loads(raw_data)

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    records = []

    for city, hourly in raw.items():
        df = pd.DataFrame(hourly)
        if df.empty:
            ti.log.warning(f"Пустые данные для {city}")
            continue

        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        for row in df.to_dict(orient='records'):
            records.append({
                'obs_ts': row.get('time'),
                'temperature': row.get('temperature_2m'),
                'rain': row.get('rain'),
                'location': city,
                'timezone': TIMEZONE,
                'api_updated_at': current_time
            })

    ti.log.info(f"Преобразовано записей: {len(records)}")
    ti.xcom_push(key='records', value=records)

def upload_data(**context):
    """Загрузка обработанных данных в S3 в формате Parquet"""
    ds = context['ds']
    ti = context['ti']
    records = ti.xcom_pull(task_ids='transform_task', key='records') or []

    if not records:
        ti.log.warning("Нет данных для загрузки")
        return

    df = pd.DataFrame(records)
    if df.empty:
        ti.log.warning("Пустой DataFrame")
        return

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    filename = f"{CATALOG_NAME}/{TABLE_NAME}/processed/weather_{ds}.parquet"
    S3Hook(S3_CONN_ID).load_bytes(
        bytes_data=buffer.read(),
        key=filename,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    ti.log.info(f"Загружено {len(df)} строк за {ds}")

# DAG параметры
default_args = {
    'owner': 'bryantsev',
    'start_date': days_ago(10),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='API_to_S3__weather_temp_rain',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=True,
    max_active_runs=1,
    description='Сбор погодных данных из Open-Meteo и сохранение в S3',
    tags=['weather', 'api', 'S3'],
    
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        doc_md="**Извлекает погодные данные по API и сохраняет в raw JSON (S3)**"
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        doc_md="**Преобразует raw JSON (S3)**"
    )

    upload_task = PythonOperator(
        task_id='upload_task',
        python_callable=upload_data,
        doc_md="**Сохраняет преобразованные данные в Parquet (S3)**"
    )

    extract_task >> transform_task >> upload_task
