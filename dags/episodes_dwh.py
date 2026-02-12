from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="episodes_dwh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "episodes", "dwh", "episode"]
) as dag:
    

    create_dwh_episode_table = PostgresOperator(
        task_id = 'create_dwh_episode',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.dim_episode (
                    episode_sk SERIAL NOT NULL,                      -- surrogate key для SCD2
                    episode_id int NOT NULL,                         -- business key
                    episode_url text NULL,                           -- дополнительный BK
                    "name" text NULL,
                    episode text NULL,
                    air_date date NULL,
                    source_created_at timestamptz NULL,
                    created_at timestamp with time zone NOT NULL,   -- дата создания версии в DWH
                    last_upd_at timestamp with time zone NOT NULL,  -- дата последнего обновления версии
                    valid_from timestamp with time zone NOT NULL,
                    valid_to timestamp with time zone NOT NULL,
                    is_current boolean NOT NULL DEFAULT TRUE,
                    CONSTRAINT dim_episode_pk PRIMARY KEY (episode_sk)
                );

            """
    )



    close_and_insert_dwh_episode_table_from_stg = PostgresOperator(
        task_id = 'close_and_insert_dwh_episode',
        postgres_conn_id="postgres_local",
        sql = """
                BEGIN;

                UPDATE dwh.dim_episode de
                SET 
                    valid_to = now(),
                    is_current = FALSE,
                    last_upd_at = now()
                FROM stg.episode s
                WHERE de.episode_id = s.id
                AND de.is_current = TRUE
                AND (
                        de."name" IS DISTINCT FROM s."name" OR
                        de.episode IS DISTINCT FROM s.episode OR
                        de.air_date IS DISTINCT FROM s.air_date OR
                        de.episode_url IS DISTINCT FROM s.url
                    );


                INSERT INTO dwh.dim_episode (
                    episode_id,
                    episode_url,
                    "name",
                    episode,
                    air_date,
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
                    s.episode,
                    s.air_date,
                    s.source_created_at,
                    now() AS created_at,
                    now() AS last_upd_at,
                    now() AS valid_from,
                    'infinity'::timestamptz AS valid_to,
                    TRUE AS is_current
                FROM stg.episode s
                LEFT JOIN dwh.dim_episode de
                    ON de.episode_id = s.id
                AND de.is_current = TRUE
                WHERE de.episode_sk IS NULL  -- новые эпизоды
                OR de."name" IS DISTINCT FROM s."name"
                OR de.episode IS DISTINCT FROM s.episode
                OR de.air_date IS DISTINCT FROM s.air_date
                OR de.episode_url IS DISTINCT FROM s.url;

                COMMIT;


            """
    )




create_dwh_episode_table >> close_and_insert_dwh_episode_table_from_stg
