from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="dwh_fact_character_episode",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "dwh", "character", "episode", "character_episode", "fact"]
) as dag:

    create_dwh_fact_character_episode_table = PostgresOperator(
        task_id = 'create_dwh_fact_character_episode',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.fact_char_ep ( 
                    character_sk bigint NOT NULL,
                    episode_sk   bigint NOT NULL,
                    created_at   timestamptz NOT NULL,

                    CONSTRAINT fact_char_ep_pk 
                        PRIMARY KEY (character_sk, episode_sk),

                    CONSTRAINT fact_char_ep_character_fk 
                        FOREIGN KEY (character_sk) 
                        REFERENCES dwh.dim_character (character_sk),

                    CONSTRAINT fact_char_ep_episode_fk 
                        FOREIGN KEY (episode_sk) 
                        REFERENCES dwh.dim_episode (episode_sk)
                );

            """
    )

    insert_dwh_fact_character_episode = PostgresOperator(
        task_id = 'insert_dwh_fact_character_episode',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO dwh.fact_char_ep (
                    character_sk,
                    episode_sk,
                    created_at
                )

                WITH unified AS (

                    -- Источник 1: character → episodes
                    SELECT
                        ec.id AS character_id,
                        e.id  AS episode_id
                    FROM stg.episode_ch ec
                    JOIN stg.episode e
                        ON e.url = ec.url

                    UNION

                    -- Источник 2: episode → characters
                    SELECT
                        c.id  AS character_id,
                        ce.id AS episode_id
                    FROM stg.character_ep ce
                    JOIN stg.character c
                        ON c.url = ce.url
                ),

                deduplicated AS (
                    SELECT DISTINCT
                        character_id,
                        episode_id
                    FROM unified
                ),

                resolved AS (
                    SELECT
                        dc.character_sk,
                        de.episode_sk
                    FROM deduplicated d
                    JOIN dwh.dim_character dc
                        ON dc.character_id = d.character_id
                    AND dc.is_current = true
                    JOIN dwh.dim_episode de
                        ON de.episode_id = d.episode_id
                    AND de.is_current = true
                )

                SELECT
                    r.character_sk,
                    r.episode_sk,
                    now()
                FROM resolved r

                -- Инкрементальность (anti-join)
                LEFT JOIN dwh.fact_char_ep f
                    ON f.character_sk = r.character_sk
                AND f.episode_sk   = r.episode_sk

                WHERE f.character_sk IS NULL

                -- Safety net
                ON CONFLICT (character_sk, episode_sk) DO NOTHING;

        """
    )



create_dwh_fact_character_episode_table >> insert_dwh_fact_character_episode