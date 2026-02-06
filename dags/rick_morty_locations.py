from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import ramapi
from psycopg2.extras import Json

def extract_raw_locations(**context):
    try:
        response = ramapi.Location.get_all()
        result_api = response.get("results", [])
    except Exception as e:
        logging.info("Failed to fetch locations from API")
        raise e
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    rows = [
        (location["id"], Json(location))
        for location in result_api
    ]
    cur.executemany("""
        INSERT INTO raw.location (source_id, payload)
        VALUES (%s, %s)
        ON CONFLICT (source_id) DO UPDATE
        SET payload = EXCLUDED.payload;
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data inserted into Postgres raw.location successfully.")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="rick_morty_locations_api_raw_stg",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "locations", "raw", "stg", "api"]
) as dag:
    
    create_raw_characters_table = PostgresOperator(
        task_id = 'create_raw_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS raw.location (
                source_id INT PRIMARY KEY,     -- id из API
                payload   JSONB NOT NULL,       -- весь JSON как есть
                loaded_at TIMESTAMPTZ DEFAULT now()
            );
            """
    )

    insert_raw_db = PythonOperator(
        task_id="insert_raw_locations",
        python_callable=extract_raw_locations,
        provide_context=True
    )

    create_stg_locations_table = PostgresOperator(
        task_id = 'create_stg_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.location ( 
                    id int NOT NULL, 
                    name text NULL,  
                    type text NULL, 
                    dimension text NULL, 
                    url text NULL, 
                    source_created_at timestamp with time zone NULL,
                    loaded_at timestamp with time zone  NOT NULL,
                    last_upd_at timestamp with time zone  NOT null,
                    CONSTRAINT location_pk PRIMARY KEY (id) );
            """
    )

    insert_stg_locations_from_raw = PostgresOperator(
        task_id = 'insert_stg_location',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.location
                SELECT 
                    (payload ->> 'id')::int         as id,
                    payload ->> 'name'              as name,
                    payload ->> 'type'              as type,
                    payload ->> 'dimension'            as dimension,
                    payload ->> 'url'               as url,
                    (payload ->> 'created')::timestamp with time zone     as source_created_at,
                    now()                           as loaded_at,
                    now()                           as last_upd_at
                FROM raw.location
                ON CONFLICT (id) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    dimension = EXCLUDED.dimension,
                    url = EXCLUDED.url,
                    last_upd_at = now()
                WHERE stg.location.name IS DISTINCT FROM EXCLUDED.name
                        OR stg.location.type IS DISTINCT FROM EXCLUDED.type
                        OR stg.location.dimension IS DISTINCT FROM EXCLUDED.dimension
                        OR stg.location.url IS DISTINCT FROM EXCLUDED.url;
            """
    )

    create_stg_character_loc_table = PostgresOperator(
        task_id = 'create_stg_character_loc',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.character_loc ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT character_loc_pk PRIMARY KEY (id, url) );
            """
    )

    insert_stg_character_loc_from_raw = PostgresOperator(
        task_id = 'insert_stg_character_loc',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.character_loc
                SELECT
                    (payload ->> 'id')::int AS id,
                    jsonb_array_elements_text(payload -> 'residents') AS url,
                    now()                               as loaded_at
                FROM raw.location
                ON CONFLICT (id, url) DO NOTHING;
            """
    )







create_raw_characters_table >> insert_raw_db >> [create_stg_locations_table, create_stg_character_loc_table]

create_stg_locations_table >> insert_stg_locations_from_raw
create_stg_character_loc_table >> insert_stg_character_loc_from_raw





