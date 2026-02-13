from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago



default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
    
}

with DAG(
    dag_id="character_stg",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "characters", "stg", "character"]
) as dag:


    create_stg_character_table = PostgresOperator(
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

    insert_stg_character_from_raw = PostgresOperator(
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
                    role text NOT NULL,         -- 'origin' | 'last'
                    loaded_at timestamp with time zone  NOT NULL,
                    last_upd_at timestamp with time zone  NOT null,
                    CONSTRAINT location_ch_pk PRIMARY KEY (id, role) );
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
                    NULLIF(TRIM(payload -> 'location' ->> 'url'), '') AS url,
                    'last'                              as role,
                    now()                               as loaded_at,
                    now()                               as last_upd_at
                FROM raw.character
                WHERE payload -> 'location' ->> 'url' IS NOT NULL
                ON CONFLICT (id, role) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    last_upd_at = now()
                WHERE stg.location_ch.name IS DISTINCT FROM EXCLUDED.name
                        OR stg.location_ch.url IS DISTINCT FROM EXCLUDED.url;
            """
    )

    insert_origin_char_from_raw = PostgresOperator(
        task_id = 'insert_stg_origin_ch',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO stg.location_ch
                SELECT 
                    (payload ->> 'id')::int             as id,
                    payload -> 'origin' ->> 'name'      as name,
                    NULLIF(TRIM(payload -> 'origin' ->> 'url'), '') AS url,
                    'origin'                            as role,
                    now()                               as loaded_at,
                    now()                               as last_upd_at
                FROM raw.character
                WHERE payload -> 'origin' ->> 'url' IS NOT NULL
                ON CONFLICT (id, role) DO UPDATE
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





[create_stg_character_table, create_stg_location_char_table, create_stg_episode_char_table]

create_stg_character_table >> insert_stg_character_from_raw
create_stg_location_char_table >> [insert_location_char_from_raw, insert_origin_char_from_raw]
create_stg_episode_char_table >> insert_stg_episode_char_from_raw

