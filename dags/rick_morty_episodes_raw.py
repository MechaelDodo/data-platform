from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
import pandas as pd
import logging
import ramapi
from psycopg2.extras import execute_values, Json
from urllib.parse import urlparse, parse_qs
import requests


def extract_raw_episodes(**context):
    
    total_inserted = 0
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    def get_next_page(response: dict, conn, cur, next_url=None):
        nonlocal total_inserted
        result_api = response.get('results', [])
        rows = [
                (episode["id"], Json(episode))
                for episode in result_api
        ]
        cur.executemany("""
            INSERT INTO raw.episode (source_id, payload)
            VALUES (%s, %s)
            ON CONFLICT (source_id) DO UPDATE
            SET payload = EXCLUDED.payload;
        """, rows)
        conn.commit()
        inserted = len(rows)
        total_inserted += inserted
        logging.info(f"Inserted {inserted} episodes.")
        if not next_url:
            return
        else:
            response_next = requests.get(next_url).json()
            next_url = response_next.get('info', {}).get('next', None)
            get_next_page(response_next, conn, cur, next_url)

    try:
        try:
            response = ramapi.Episode.get_all()
        except Exception as e:
            logging.info("Failed to fetch episodes from API")
            raise e
        next_url = response.get('info', {}).get('next', None)
        get_next_page(response, conn, cur, next_url) 
    except Exception as e:
        logging.info("Failed to fetch episodes from API")
        raise e
    finally:
        cur.close()
        conn.close()
    logging.info(f"Finished loading episodes. Total inserted: {total_inserted}")



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
    
}

with DAG(
    dag_id="rick_morty_episodes_api_raw",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "episodes", "raw", "api"]
) as dag:
    
    create_raw_episode_table = PostgresOperator(
        task_id = 'create_raw_episode',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS raw.episode (
                source_id INT PRIMARY KEY,     -- id из API
                payload   JSONB NOT NULL,       -- весь JSON как есть
                loaded_at TIMESTAMPTZ DEFAULT now()
            );
            """
    )
    
    insert_raw_db = PythonOperator(
        task_id="insert_raw_episodes",
        python_callable=extract_raw_episodes,
        provide_context=True
    )


create_raw_episode_table >> insert_raw_db 