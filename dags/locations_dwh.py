from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="location_dwh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "locations", "location", "dwh"]
) as dag:
    

    create_dwh_location_table = PostgresOperator(
        task_id = 'create_dwh_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.dim_location (
                    location_sk SERIAL NOT NULL,                    -- surrogate key для SCD2
                    location_id int NOT NULL,                       -- business key
                    location_url text NULL,                         -- дополнительный BK
                    "name" text NULL,
                    "type" text NULL,
                    dimension text NULL,
                    source_created_at timestamptz NULL,
                    created_at timestamp with time zone NOT NULL,   -- дата создания версии в DWH
                    last_upd_at timestamp with time zone NOT NULL,  -- дата последнего обновления версии
                    valid_from timestamp with time zone NOT NULL,
                    valid_to timestamp with time zone NOT NULL,
                    is_current boolean NOT NULL DEFAULT TRUE,
                    CONSTRAINT dim_location_pk PRIMARY KEY (location_sk)
                );

            """
    )



    close_and_insert_dwh_location_table_from_stg = PostgresOperator(
        task_id = 'close_and_insert_dwh_location',
        postgres_conn_id="postgres_local",
        sql = """
                BEGIN;

                UPDATE dwh.dim_location dl
                SET valid_to = now(),
                    is_current = FALSE,
                    last_upd_at = now()
                FROM stg."location" s
                WHERE dl.location_id = s.id
                AND dl.is_current = TRUE
                AND (
                        dl."name" IS DISTINCT FROM s."name" OR
                        dl."type" IS DISTINCT FROM s."type" OR
                        dl.dimension IS DISTINCT FROM s.dimension OR
                        dl.location_url IS DISTINCT FROM s.url
                    );

                INSERT INTO dwh.dim_location (
                    location_id,
                    location_url,
                    "name",
                    "type",
                    dimension,
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
                    s."type",
                    s.dimension,
                    s.source_created_at,
                    now() as created_at,
                    now() as last_upd_at,
                    now() as valid_from,
                    'infinity'::timestamp with time zone as valid_to,
                    TRUE as is_current
                FROM stg."location" s
                LEFT JOIN dwh.dim_location dl
                ON dl.location_id = s.id AND dl.is_current = TRUE
                WHERE dl.location_sk IS NULL
                OR dl."name" IS DISTINCT FROM s."name"
                OR dl."type" IS DISTINCT FROM s."type"
                OR dl.dimension IS DISTINCT FROM s.dimension
                OR dl.location_url IS DISTINCT FROM s.url;

                COMMIT;

            """
    )




create_dwh_location_table >> close_and_insert_dwh_location_table_from_stg
