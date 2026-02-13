from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from operators.extract_api_operator import InsertApiToRawOperator


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
    
}

with DAG(
    dag_id="raw_episode",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["rick_morty", "raw", "api", "episode"]
) as dag:
    
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
    
    insert_raw_episode = InsertApiToRawOperator(
        task_id="insert_raw_episode",
        entity="episode",
        postgres_conn_id="postgres_local",
    )


create_raw_episode_table >> insert_raw_episode 