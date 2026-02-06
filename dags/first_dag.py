# ./dags/airflow_playground.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import logging

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 5,  # seconds
}

dag = DAG(
    dag_id="airflow_playground",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    tags=["practice", "playground"]
)

# -------------------
# Таск 1: работа с переменными
# -------------------
def variable_task(**context):
    # Получаем переменную greeting или создаем новую
    greeting = Variable.get("greeting", default_var="Hello Airflow!")
    logging.info(f"Variable 'greeting': {greeting}")
    
    # Обновляем переменную
    Variable.set("greeting", greeting + " 👋")
    
    # Отправляем значение через XCom
    return greeting

task_variable = PythonOperator(
    task_id="variable_task",
    python_callable=variable_task,
    provide_context=True,
    dag=dag
)

# -------------------
# Таск 2: создаем DataFrame и сохраняем в Postgres
# -------------------
def create_and_insert_df(**context):
    # Получаем greeting из XCom предыдущей таски
    greeting = context['ti'].xcom_pull(task_ids='variable_task')
    
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Rick", "Morty", "Summer"],
        "greeting": [greeting]*3
    })
    
    logging.info(f"DataFrame to insert:\n{df}")
    
    # Подключаемся к Postgres через Airflow Hook
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")  # имя connection в Airflow UI
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Создаем временную таблицу
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg.playground (
            id INT PRIMARY KEY,
            name TEXT,
            greeting TEXT
        )
    """)
    
    # Вставляем данные с ON CONFLICT DO UPDATE
    from psycopg2.extras import execute_values
    records = df.to_dict('records')
    values = [
        (
            r['id'],
            r['name'],
            r['greeting']
        )
        for r in records
        ]
    execute_values(
        cur,
        """
        INSERT INTO stg.playground (id, name, greeting) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            greeting = EXCLUDED.greeting
        """,
        values
    )
    conn.commit()
    cur.close()
    logging.info("Data inserted into Postgres successfully.")

task_insert_db = PythonOperator(
    task_id="create_and_insert_df",
    python_callable=create_and_insert_df,
    provide_context=True,
    dag=dag
)

# -------------------
# Таск 3: читаем таблицу и логируем
# -------------------
def read_from_db(**context):
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM stg.playground;")
    rows = cur.fetchall()
    logging.info(f"Data from Postgres:\n{rows}")
    
    cur.close()

task_read_db = PythonOperator(
    task_id="read_from_db",
    python_callable=read_from_db,
    provide_context=True,
    dag=dag
)

# -------------------
# Зависимости
# -------------------
task_variable >> task_insert_db >> task_read_db
