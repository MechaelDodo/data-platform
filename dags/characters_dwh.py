from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="characters_dwh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "characters", "dwh"]
) as dag:
    

    create_dwh_character_table = PostgresOperator(
        task_id = 'create_dwh_character',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.dim_character (
                    character_sk SERIAL NOT NULL,                   -- surrogate key для SCD2
                    character_id int NOT NULL,                      -- business key
                    character_url text NULL,                        -- дополнительный BK
                    "name" text NULL,
                    status text NULL,
                    species text NULL,
                    "type" text NULL,
                    gender text NULL,
                    image text NULL,
                    source_created_at timestamp with time zone NULL,
                    created_at timestamp with time zone NOT NULL,   -- дата создания версии в DWH
                    last_upd_at timestamp with time zone NOT NULL,  -- дата последнего обновления версии
                    valid_from timestamp with time zone NOT NULL,
                    valid_to timestamp with time zone NOT NULL,
                    is_current boolean NOT NULL DEFAULT TRUE,
                    CONSTRAINT dim_character_pk PRIMARY KEY (character_sk)
                );
            """
    )



    close_and_insert_dwh_character_table_from_stg = PostgresOperator(
        task_id = 'close_and_insert_dwh_character',
        postgres_conn_id="postgres_local",
        sql = """
                BEGIN;
                
                UPDATE dwh.dim_character dc
                SET valid_to = now(),
                    is_current = FALSE,
                    last_upd_at = now()
                FROM stg."character" s
                WHERE dc.character_id = s.id
                AND dc.is_current = TRUE
                AND (
                        dc."name" IS DISTINCT FROM s."name" OR
                        dc.status IS DISTINCT FROM s.status OR
                        dc.species IS DISTINCT FROM s.species OR
                        dc."type" IS DISTINCT FROM s."type" OR
                        dc.gender IS DISTINCT FROM s.gender OR
                        dc.image IS DISTINCT FROM s.image OR
                        dc.character_url IS DISTINCT FROM s.url
                    );

                
                INSERT INTO dwh.dim_character (
                    character_id,
                    character_url,
                    "name",
                    status,
                    species,
                    "type",
                    gender,
                    image,
                    source_created_at,
                    created_at,
                    last_upd_at,
                    valid_from,
                    valid_to,
                    is_current
                )
                SELECT 
                    s.id,
                    s.url,
                    s."name",
                    s.status,
                    s.species,
                    s."type",
                    s.gender,
                    s.image,
                    s.source_created_at,
                    now() as created_at,
                    now() as last_upd_at,
                    now() as valid_from,
                    'infinity'::timestamp with time zone as valid_to,
                    TRUE as is_current
                FROM stg."character" s
                LEFT JOIN dwh.dim_character dc
                ON dc.character_id = s.id AND dc.is_current = TRUE
                WHERE dc.character_sk IS NULL  -- новые персонажи
                OR dc."name" IS DISTINCT FROM s."name"
                OR dc.status IS DISTINCT FROM s.status
                OR dc.species IS DISTINCT FROM s.species
                OR dc."type" IS DISTINCT FROM s."type"
                OR dc.gender IS DISTINCT FROM s.gender
                OR dc.image IS DISTINCT FROM s.image
                OR dc.character_url IS DISTINCT FROM s.url;

                COMMIT;
            """
    )




create_dwh_character_table >> close_and_insert_dwh_character_table_from_stg
