from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
import pandas as pd
import logging
import ramapi
from psycopg2.extras import Json


def extract_raw_episodes(**context):
    try:
        response = ramapi.Episode.get_all()
        result_api = response.get("results", [])
    except Exception as e:
        logging.info("Failed to fetch episodes from API")
        raise e
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
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
    # for episode in result_api:
    #     cur.execute(
    #         """
    #         INSERT INTO raw.episode (source_id, payload)
    #         VALUES (%s, %s)
    #         ON CONFLICT (source_id) DO UPDATE
    #         SET payload = EXCLUDED.payload,
    #             loaded_at = now();
    #         """,
    #         (
    #             episode["id"],     # source_id
    #             Json(episode),     # <-- ВАЖНО
    #         )
    #     )
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data inserted into Postgres raw.episode successfully.")



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="rick_morty_episodes_api_raw_stg",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "episodes", "raw", "stg", "api"]
) as dag:
    
    create_raw_episodes_table = PostgresOperator(
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


    create_stg_episodes_table = PostgresOperator(
        task_id = 'create_stg_episode',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.episode ( 
                    id int NOT NULL, 
                    name text NULL,  
                    episode text NULL, 
                    air_date date NULL, 
                    url text NULL, 
                    source_created_at timestamp with time zone NULL,
                    loaded_at timestamp with time zone  NOT NULL,
                    last_upd_at timestamp with time zone  NOT null,
                    CONSTRAINT episode_pk PRIMARY KEY (id) );
            """
    )

    insert_stg_episodes_from_raw = PostgresOperator(
        task_id = 'insert_stg_episode',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.episode
                SELECT 
                    (payload ->> 'id')::int         as id,
                    payload ->> 'name'              as name,
                    payload ->> 'episode'              as episode,
                    CASE
                        WHEN payload ->> 'air_date' ~ '^[A-Za-z]+ [0-9]{1,2}, [0-9]{4}$'
                            THEN to_date(payload ->> 'air_date', 'Month DD, YYYY')
                        ELSE NULL
                    END                             as air_date,
                    payload ->> 'url'               as url,
                    (payload ->> 'created')::timestamp with time zone     as source_created_at,
                    now()                           as loaded_at,
                    now()                           as last_upd_at
                FROM raw.episode
                ON CONFLICT (id) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    episode = EXCLUDED.episode,
                    air_date = EXCLUDED.air_date,
                    url = EXCLUDED.url,
                    last_upd_at = now()
                WHERE stg.episode.name IS DISTINCT FROM EXCLUDED.name
                        OR stg.episode.episode IS DISTINCT FROM EXCLUDED.episode
                        OR stg.episode.air_date IS DISTINCT FROM EXCLUDED.air_date
                        OR stg.episode.url IS DISTINCT FROM EXCLUDED.url;
            """
    )


    create_stg_character_ep_table = PostgresOperator(
        task_id = 'create_stg_character_ep',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.character_ep ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT character_ep_pk PRIMARY KEY (id, url) );
            """
    )

    insert_stg_character_ep_from_raw = PostgresOperator(
        task_id = 'insert_stg_character_ep',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.character_ep
                SELECT
                    (payload ->> 'id')::int AS id,
                    jsonb_array_elements_text(payload -> 'characters') AS url,
                    now()                               as loaded_at
                FROM raw.episode
                ON CONFLICT (id, url) DO NOTHING;
            """
    )



create_raw_episodes_table >> insert_raw_db >> [create_stg_episodes_table, create_stg_character_ep_table]

create_stg_episodes_table >> insert_stg_episodes_from_raw
create_stg_character_ep_table >> insert_stg_character_ep_from_raw
