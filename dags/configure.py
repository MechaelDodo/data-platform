from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

with DAG(
    dag_id="configure",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "create", "scheme", "table", "configure"]
) as dag:
    

    create_scheme_raw = PostgresOperator(
        task_id = 'create_scheme_raw',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE SCHEMA IF NOT EXISTS raw;
            """
    )

    create_scheme_stg = PostgresOperator(
        task_id = 'create_scheme_stg',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE SCHEMA IF NOT EXISTS stg;
            """
    )

    create_scheme_dwh = PostgresOperator(
        task_id = 'create_scheme_dwh',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE SCHEMA IF NOT EXISTS dwh;
            """
    )

    create_scheme_datamart = PostgresOperator(
        task_id = 'create_scheme_datamart',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE SCHEMA IF NOT EXISTS datamart;
            """
    )

    create_raw_character_table = PostgresOperator(
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

    create_raw_location_table = PostgresOperator(
        task_id = 'create_raw_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS raw.location (
                source_id INT PRIMARY KEY,     -- id из API
                payload   JSONB NOT NULL,       -- весь JSON как есть
                loaded_at TIMESTAMPTZ DEFAULT now()
            );
            """
    )

    create_raw_episode_table = PostgresOperator(
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

    create_stg_location_character_table = PostgresOperator(
        task_id = 'create_stg_location_character',
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

    create_stg_episode_character_table = PostgresOperator(
        task_id = 'create_stg_episode_character',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.episode_ch ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT episode_ch_pk PRIMARY KEY (id, url) );
            """
    )

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

    create_stg_character_location_table = PostgresOperator(
        task_id = 'create_stg_character_location',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.character_loc ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT character_loc_pk PRIMARY KEY (id, url) );
            """
    )

    create_stg_episode_table = PostgresOperator(
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

    create_stg_character_episode_table = PostgresOperator(
        task_id = 'create_stg_character_episode',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS stg.character_ep ( 
                    id int NOT NULL, 
                    url text NULL, 
                    loaded_at timestamp with time zone  NOT NULL,
                    CONSTRAINT character_ep_pk PRIMARY KEY (id, url) );
            """
    )

    create_dwh_dim_character_table = PostgresOperator(
        task_id = 'create_dwh_dim_character',
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

    create_dwh_dim_location_table = PostgresOperator(
        task_id = 'create_dwh_dim_location',
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


    create_dwh_dim_episode_table = PostgresOperator(
        task_id = 'create_dwh_dim_episode',
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

    insert_dwh_dim_location = PostgresOperator(
        task_id = 'insert_dwh_dim_location',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO dwh.dim_location (
                    location_id,
                    location_url,
                    name,
                    type,
                    dimension,
                    source_created_at,
                    created_at,
                    last_upd_at,
                    valid_from,
                    valid_to,
                    is_current
                )
                SELECT
                    -1,
                    NULL,
                    'Unknown',
                    NULL,
                    NULL,
                    now(),
                    now(),
                    now(),
                    now(),
                    'infinity'::timestamptz,
                    true
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM dwh.dim_location
                    WHERE location_id = -1
                );

            """
    )

    create_dwh_dim_location_role_table = PostgresOperator(
        task_id = 'create_dwh_dim_location_role',
        postgres_conn_id="postgres_local",
        sql = """
                CREATE TABLE IF NOT EXISTS dwh.dim_location_role (
                    role_id int2 NOT NULL,
                    role_name text NOT NULL,
                    CONSTRAINT dim_location_role_pk PRIMARY KEY (role_id),
                    CONSTRAINT dim_location_role_role_name_uk UNIQUE (role_name)
                );

            """
    )

    insert_dwh_dim_location_role = PostgresOperator(
        task_id = 'insert_dwh_dim_location_role',
        postgres_conn_id="postgres_local",
        sql = """
                INSERT INTO dwh.dim_location_role (role_id, role_name)
                VALUES
                    (1, 'origin'),
                    (2, 'last')
                ON CONFLICT (role_id) DO NOTHING;
            """
    )

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


[create_scheme_raw, create_scheme_stg, create_scheme_dwh, create_scheme_datamart]


create_scheme_raw >> [create_raw_character_table, 
                      create_raw_location_table, 
                      create_raw_episode_table]
create_scheme_stg >> [create_stg_character_table, create_stg_location_character_table, create_stg_episode_character_table,
                      create_stg_location_table, create_stg_character_location_table,
                      create_stg_episode_table, create_stg_character_episode_table]


create_scheme_dwh >> [create_dwh_dim_character_table, 
                      create_dwh_dim_location_table,
                      create_dwh_dim_episode_table]  >> insert_dwh_dim_location >> create_dwh_dim_location_role_table >> insert_dwh_dim_location_role >> [create_dwh_fact_character_location_table,
                                                                                                                                                         create_dwh_fact_character_episode_table]