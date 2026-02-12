from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import ramapi
import logging
from psycopg2.extras import Json
import requests

ENTITY_CLIENTS = {
    "episode": ramapi.Episode,
    "location": ramapi.Location,
}

class InsertApiToRawOperator(BaseOperator):

    def __init__(self, entity, postgres_conn_id="postgres_local", **kwargs):
        super().__init__(**kwargs)
        if entity not in ENTITY_CLIENTS:
            raise ValueError(f"Unsupported entity: {entity}")
        self.entity = entity
        self.postgres_conn_id = postgres_conn_id


    def execute(self, context):
        total_inserted = 0
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        def get_next_page(response: dict, conn, cur, next_url=None):
            nonlocal total_inserted
            result_api = response.get('results', [])
            rows = [
                    (episode["id"], Json(episode))
                    for episode in result_api
            ]
            sql_insert = f"""
                INSERT INTO raw.{self.entity} (source_id, payload)
                VALUES (%s, %s)
                ON CONFLICT (source_id) DO UPDATE
                SET payload = EXCLUDED.payload;
            """
            cur.executemany(sql_insert, rows)
            conn.commit()
            inserted = len(rows)
            total_inserted += inserted
            self.log.info(f"Inserted {inserted} {self.entity}s.")
            if not next_url:
                return
            else:
                response_next = requests.get(next_url).json()
                next_url = response_next.get('info', {}).get('next', None)
                get_next_page(response_next, conn, cur, next_url)



        try:
            try:
                client = ENTITY_CLIENTS.get(self.entity)
                if not client:
                    raise ValueError("Failed getting entity from ramapi")
                response = client.get_all()
            except Exception as e:
                self.log.info(f"Failed to fetch {self.entity}s from API")
                raise e
            next_url = response.get('info', {}).get('next', None)
            get_next_page(response, conn, cur, next_url) 
        except Exception as e:
            self.log.info(f"Failed to fetch {self.entity}s from API")
            raise e
        finally:
            cur.close()
            conn.close()
        self.log.info(f"Finished loading {self.entity}s. Total inserted: {total_inserted}")