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


def extract_raw_characters(**context):
    try:
        response = ramapi.Character.get_all()
        result_api = response.get("results", [])
    except Exception as e:
        logging.info("Failed to fetch characters from API")
        raise e
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
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
    cur.close()
    conn.close()
    logging.info("Data inserted into Postgres raw.character successfully.")



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
    
}

with DAG(
    dag_id="rick_morty_characters_api_raw_stg",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "characters", "raw", "stg", "api"]
) as dag:
    
    create_raw_characters_table = PostgresOperator(
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
    
    insert_raw_db = PythonOperator(
        task_id="insert_raw_characters",
        python_callable=extract_raw_characters,
        provide_context=True
    )


    create_stg_characters_table = PostgresOperator(
        task_id = 'create_stg_character',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.character ( 
                    id int NOT NULL, 
                    name text NULL, 
                    status text NULL, 
                    species text NULL, 
                    type text NULL, 
                    gender text NULL, 
                    image text NULL, 
                    url text NULL, 
                    source_created_at timestamp with time zone NULL,
                    loaded_at timestamp with time zone  NOT NULL,
                    last_upd_at timestamp with time zone  NOT null,
                    CONSTRAINT character_pk PRIMARY KEY (id) );
            """
    )

    insert_stg_characters_from_raw = PostgresOperator(
        task_id = 'insert_stg_character',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.character
                SELECT 
                    (payload ->> 'id')::int         as id,
                    payload ->> 'name'              as name,
                    payload ->> 'status'            as status,
                    payload ->> 'species'           as species,
                    payload ->> 'type'              as type,
                    payload ->> 'gender'            as gender,
                    payload ->> 'image'             as image,
                    payload ->> 'url'               as url,
                    (payload ->> 'created')::timestamp with time zone     as source_created_at,
                    now()                           as loaded_at,
                    now()                           as last_upd_at
                FROM raw.character
                ON CONFLICT (id) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    status = EXCLUDED.status,
                    species = EXCLUDED.species,
                    type = EXCLUDED.type,
                    gender = EXCLUDED.gender,
                    image = EXCLUDED.image,
                    url = EXCLUDED.url,
                    last_upd_at = now()
                WHERE stg.character.name IS DISTINCT FROM EXCLUDED.name
                        OR stg.character.status IS DISTINCT FROM EXCLUDED.status
                        OR stg.character.species IS DISTINCT FROM EXCLUDED.species
                        OR stg.character.type IS DISTINCT FROM EXCLUDED.type
                        OR stg.character.gender IS DISTINCT FROM EXCLUDED.gender
                        OR stg.character.image IS DISTINCT FROM EXCLUDED.image
                        OR stg.character.url IS DISTINCT FROM EXCLUDED.url;
            """
    )

    create_stg_location_char_table = PostgresOperator(
        task_id = 'create_stg_location_ch',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.location_ch ( 
                    id int NOT NULL, 
                    name text NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    last_upd_at timestamp with time zone  NOT null,
                    CONSTRAINT location_ch_pk PRIMARY KEY (id) );
            """
    )

    insert_location_char_from_raw = PostgresOperator(
        task_id = 'insert_stg_location_ch',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.location_ch
                SELECT 
                    (payload ->> 'id')::int             as id,
                    payload -> 'location' ->> 'name'    as name,
                    payload-> 'location' ->> 'url'      as url,
                    now()                               as loaded_at,
                    now()                               as last_upd_at
                FROM raw.character
                ON CONFLICT (id) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    last_upd_at = now()
                WHERE stg.location_ch.name IS DISTINCT FROM EXCLUDED.name
                        OR stg.location_ch.url IS DISTINCT FROM EXCLUDED.url;
            """
    )


    create_stg_episode_char_table = PostgresOperator(
        task_id = 'create_stg_episode_ch',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.episode_ch ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT episode_ch_pk PRIMARY KEY (id, url) );
            """
    )

    insert_stg_episode_char_from_raw = PostgresOperator(
        task_id = 'insert_stg_episode_ch',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.episode_ch
                SELECT
                    (payload ->> 'id')::int AS id,
                    jsonb_array_elements_text(payload -> 'episode') AS url,
                    now()                               as loaded_at
                FROM raw.character
                ON CONFLICT (id, url) DO NOTHING;
            """
    )





create_raw_characters_table >> insert_raw_db >> [create_stg_characters_table, create_stg_location_char_table, create_stg_episode_char_table]

create_stg_characters_table >> insert_stg_characters_from_raw
create_stg_location_char_table >> insert_location_char_from_raw
create_stg_episode_char_table >> insert_stg_episode_char_from_raw
