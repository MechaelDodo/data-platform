from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="dwh_fact_character_location",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "dwh", "character", "location", "character_location", "fact"]
) as dag:

    create_dwh_fact_character_location_table = PostgresOperator(
        task_id = 'create_dwh_fact_character_location',
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

    insert_dwh_fact_character_location = PostgresOperator(
        task_id = 'insert_dwh_fact_character_location',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO dwh.fact_char_loc (
                    character_sk,
                    location_sk,
                    role_id,
                    created_at
                )

                WITH source_1 AS (
                    -- character endpoint (origin + last)
                    SELECT
                        slc.id AS character_id,
                        NULLIF(TRIM(slc.url), '') AS location_url,
                        slc.role
                    FROM stg.location_ch slc
                ),

                source_2 AS (
                    -- location endpoint (residents = last)
                    SELECT
                        c.id AS character_id,
                        l.url AS location_url,
                        'last' AS role
                    FROM stg.character_loc cl
                    JOIN stg.character c
                        ON c.url = cl.url
                    JOIN stg.location l
                        ON l.id = cl.id
                ),

                unified AS (
                    SELECT * FROM source_1
                    UNION
                    SELECT * FROM source_2
                ),

                deduplicated AS (
                    SELECT DISTINCT
                        character_id,
                        location_url,
                        role
                    FROM unified
                ),

                resolved AS (
                    SELECT
                        dc.character_sk,
                        dl.location_sk,
                        r.role_id
                    FROM deduplicated u

                    JOIN dwh.dim_character dc
                        ON dc.character_id = u.character_id
                    AND dc.is_current = true

                    JOIN dwh.dim_location dl
                        ON (
                            dl.location_url = u.location_url
                            OR (u.location_url IS NULL AND dl.location_id = -1)
                        )
                    AND dl.is_current = true

                    JOIN dwh.dim_location_role r
                        ON r.role_name = u.role
                )

                SELECT
                    r.character_sk,
                    r.location_sk,
                    r.role_id,
                    now()

                FROM resolved r

                -- Инкрементальность
                LEFT JOIN dwh.fact_char_loc f
                    ON f.character_sk = r.character_sk
                AND f.location_sk  = r.location_sk
                AND f.role_id      = r.role_id

                WHERE f.character_sk IS NULL

                -- Safety net
                ON CONFLICT (character_sk, location_sk, role_id)
                DO NOTHING;


        """
    )



create_dwh_fact_character_location_table >> insert_dwh_fact_character_location