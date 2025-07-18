from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
import requests

import pandas as pd

# Словарь городов и их координат (широта, долгота)
CITIES = {
    "Москва": (55.7558, 37.6173),
    "Санкт-Петербург": (59.9343, 30.3351),
    "Екатеринбург": (56.8389, 60.6057),
    "Казань": (55.7963, 49.1084),
}

# URL и параметры для API Open-Meteo
API_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Asia/Yekaterinburg"

# Параметры подключения к PostgreSQL
POSTGRES_CONN_ID = "postgres_conn" # Admin - Connections
SCHEMA_NAME = "bryantsev"
TABLE_NAME = "open_meteo"

# SQL для создания схемы и таблицы, если они не существуют
CREATE_TABLE_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    obs_ts TIMESTAMP NOT NULL,
    timezone VARCHAR(50),
    location VARCHAR(50),
    temperature REAL NOT NULL,
    rain REAL NOT NULL,
    pg_updated_at DATE DEFAULT CURRENT_DATE,
    data_source VARCHAR(50) DEFAULT 'api.open-meteo.com',
    api_updated_at TIMESTAMP,
    PRIMARY KEY (obs_ts, timezone, location)
);
"""

def create_table():
    """
    Создает схему и таблицу в PostgreSQL, если их нет
    """
    PostgresHook(POSTGRES_CONN_ID).run(CREATE_TABLE_SQL)

def extract_data():
    """
    Извлекает погодные данные по часам для каждого города с API Open-Meteo.
    Сохраняет результат (сырые данные) в XCom.
    """
    context = get_current_context()
    ti = context['ti']
    ds = context["ds"]  # Дата запуска в формате YYYY-MM-DD
    raw = {}

    for city, (lat, lon) in CITIES.items():
        try:
            # Запрос данных для конкретного города и даты
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
                timeout=10
            )
            resp.raise_for_status()
            raw[city] = resp.json().get("hourly", {})
            ti.log.info(f"Получены данные для города {city}")
        except Exception as e:
            ti.log.error(f"Ошибка при запросе данных для города {city}: {e}")
            raw[city] = {}  # При ошибке сохраняем пустой словарь для города

    # Сохраняем словарь с сырыми данными в XCom под ключом 'raw'
    ti.xcom_push(key='raw', value=raw)

def transform_data():
    """
    Преобразует сырые данные из XCom в список словарей для загрузки в БД.
    Добавляет необходимые поля и форматирует временные метки.
    Результат сохраняется в XCom под ключом 'records'.
    """
    context = get_current_context()
    ti = context['ti']
    raw = ti.xcom_pull(task_ids='extract_task', key='raw') or {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    records = []

    for city, hourly in raw.items():
        # Проверяем, что данные есть и они в правильном формате
        if hourly and isinstance(hourly, dict):
            df = pd.DataFrame(hourly)
            if not df.empty:
                # Форматируем столбец времени в строку
                df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')

                # Формируем записи для вставки в базу
                for row in df.itertuples(index=False):
                    records.append({
                        'obs_ts': row.time,
                        'temperature': getattr(row, 'temperature_2m', None),
                        'rain': getattr(row, 'rain', None),
                        'location': city,
                        'timezone': TIMEZONE,
                        'api_updated_at': now
                    })
            else:
                ti.log.warning(f"Пустые данные для города {city}")
        else:
            ti.log.warning(f"Нет данных для города {city}")

    # Сохраняем подготовленные записи в XCom
    ti.xcom_push(key='records', value=records)

def upload_data():
    """
    Загружает данные из XCom в таблицу PostgreSQL.
    Использует upsert по составному ключу (obs_ts, timezone, location).
    """
    context = get_current_context()
    ti = context['ti']
    records = ti.xcom_pull(task_ids='transform_task', key='records') or []

    if not records:
        ti.log.info("Нет данных для загрузки в базу.")
        return

    hook = PostgresHook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
        (obs_ts, temperature, rain, location, timezone, api_updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (obs_ts, timezone, location) DO UPDATE SET
            temperature = EXCLUDED.temperature,
            rain = EXCLUDED.rain,
            api_updated_at = EXCLUDED.api_updated_at;
    """

    try:
        data = [
            (r['obs_ts'], r['temperature'], r['rain'],
             r['location'], r['timezone'], r['api_updated_at'])
            for r in records
        ]
        cur.executemany(sql, data)
        conn.commit()
        ti.log.info(f"Успешно загружено {len(records)} записей в таблицу {SCHEMA_NAME}.{TABLE_NAME}")
    except Exception as e:
        conn.rollback()
        ti.log.error(f"Ошибка при загрузке данных: {e}")
        raise
    finally:
        cur.close()
        conn.close()

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'bryantsev',
    'start_date': days_ago(10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    dag_id='API_to_PG__weather_temp_rain',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # Запуск каждый день в 10:00 UTC
    catchup=True,
    max_active_runs=1,
    tags=['weather', 'api', 'postgres'],
    description='Загрузка погодных данных из Open-Meteo в PostgreSQL'
) as dag:
    
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        doc_md="Создает схему и таблицу в PostgreSQL, если они не существуют."
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        doc_md="Извлекает сырые погодные данные из API Open-Meteo."
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        doc_md="Преобразует сырые данные в формат, готовый для загрузки в PostgreSQL."
    )

    upload_task = PythonOperator(
        task_id='upload_task',
        python_callable=upload_data,
        doc_md="Загружает подготовленные данные в PostgreSQL с поддержкой upsert."
    )

    # Последовательность выполнения задач
    create_table >> extract_task >> transform_task >> upload_task
