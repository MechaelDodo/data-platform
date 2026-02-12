from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="fact_character_location_dwh",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "dwh", "character", "location", "character_location", "fact"]
) as dag:

    create_dwh_fct_character_location_table = PostgresOperator(
        task_id = 'create_dwh_fct_character_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.fact_char_loc ( 
                    character_sk bigint NOT NULL,
                    location_sk  bigint NOT NULL,
                    role_id      smallint NOT NULL,
                    created_at   timestamp with time zone NOT NULL,

                    CONSTRAINT fact_char_loc_pk 
                        PRIMARY KEY (character_sk, location_sk, role_id),

                    CONSTRAINT fact_char_loc_character_fk 
                        FOREIGN KEY (character_sk) 
                        REFERENCES dwh.dim_character (character_sk),

                    CONSTRAINT fact_char_loc_location_fk 
                        FOREIGN KEY (location_sk) 
                        REFERENCES dwh.dim_location (location_sk),

                    CONSTRAINT fact_char_loc_role_fk 
                        FOREIGN KEY (role_id) 
                        REFERENCES dwh.dim_location_role (role_id)
                );
            """
    )

    close_and_insert_dwh_character_table_from_stg = PostgresOperator(
        task_id = 'close_and_insert_dwh_character',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO dwh.fact_char_loc (
                    character_sk,
                    location_sk,
                    role_id,
                    created_at
                )
                SELECT DISTINCT
                    dc.character_sk,
                    dl.location_sk,
                    r.role_id,
                    now() AS created_at
                FROM stg.location_ch slc

                JOIN dwh.dim_character dc
                    ON dc.character_id = slc.id
                AND dc.is_current = true

                JOIN dwh.dim_location dl
                    ON (
                        dl.location_url = slc.url
                        OR (slc.url IS NULL AND dl.location_id = -1)
                    )
                AND dl.is_current = true

                JOIN dwh.dim_location_role r
                    ON r.role_name = slc.role

                ON CONFLICT (character_sk, location_sk, role_id)
                DO NOTHING;
        """
    )



create_dwh_fct_character_location_table >> close_and_insert_dwh_character_table_from_stg