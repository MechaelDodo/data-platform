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
import time


def extract_raw_character(**context):
    def get_page_number(next_url):
        if not next_url:
            return None
        query = urlparse(next_url).query
        params = parse_qs(query)
        return int(params['page'][0])
    
    page = 1
    total_inserted = 0
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    try:
        while page:
            try:
                response = ramapi.Character.get_page(page)
                if not response or "results" not in response:
                    logging.warning(f"Empty or invalid response on page {page}, stopping pagination")
                    break
            except Exception as e:
                logging.warning(f"Request failed on page {page}: {e}, retrying in 5s")
                time.sleep(5)
                continue  # повторяем запрос той же страницы
            info = response.get("info", {})
            result_api = response.get("results", [])

            rows = [
                (character["id"], Json(character))
                for character in result_api
            ]
            cur.executemany("""
                INSERT INTO raw.character (source_id, payload)
                VALUES (%s, %s)
                ON CONFLICT (source_id) DO UPDATE
                SET payload = EXCLUDED.payload;
            """, rows)
            conn.commit()
            inserted = len(rows)
            total_inserted += inserted
            logging.info(f"Inserted {inserted} characters from page {page}")

            page = get_page_number(info.get("next"))
    except Exception as e:
        logging.info("Failed to fetch characters from API")
        raise e
    finally:
        cur.close()
        conn.close()
    logging.info(f"Finished loading characters. Total inserted: {total_inserted}")



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
    
}

with DAG(
    dag_id="raw_character",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "raw", "api", "character"]
) as dag:
    
    create_raw_character_table = PostgresOperator(
        task_id = 'create_raw_character',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS raw.character (
                source_id INT PRIMARY KEY,     -- id из API
                payload   JSONB NOT NULL,       -- весь JSON как есть
                loaded_at TIMESTAMPTZ DEFAULT now()
            );
            """
    )
    
    insert_raw_character = PythonOperator(
        task_id="insert_raw_character",
        python_callable=extract_raw_character,
        provide_context=True
    )


create_raw_character_table >> insert_raw_character 