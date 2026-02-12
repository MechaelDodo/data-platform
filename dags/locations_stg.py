from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="location_stg",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "locations", "location", "stg"]
) as dag:
    

    create_stg_location_table = PostgresOperator(
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

    insert_stg_location_from_raw = PostgresOperator(
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







[create_stg_location_table, create_stg_character_loc_table]

create_stg_location_table >> insert_stg_location_from_raw
create_stg_character_loc_table >> insert_stg_character_loc_from_raw





