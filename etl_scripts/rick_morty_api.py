#rick_morty_api.py

import ramapi
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, Json


def get_characters():
    return ramapi.Character.get_all()['results']

def set_df_characters(characters):
    df = pd.DataFrame(characters)
    # transform
    df = df[['id',
            'name',
            'status',
            'species',
            'type',
            'gender',
            'image',  
            'url', 
            'created']]
    df["created"] = pd.to_datetime(df["created"], utc=True)
    return df

def set_df_other_attr_characters(characters):
    df = pd.DataFrame(characters)
    # transform,  have to think about NULL
    df = df[['id',
            'origin',
            'location',
            'episode']]
    return df

def set_connection():
    db_name = "test_dwh"
    user = "de_user"
    password = "de_password"
    host = "postgres"
    port = "5432"
    return psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)

def get_transform_characters(df):
    records = df.to_dict('records')  # список словарей
    values = [
    (
        r['id'],
        r['name'],
        r['status'],
        r['species'],
        r['type'],
        r['gender'],
        r['image'],
        r['url'],
        r['created']
    )
    for r in records
    ]
    return values

def insert_characters(cur, values):
    execute_values(
    cur,
    """
    INSERT INTO stg.characters_rm
    (id, name, status, species, type, gender, image, url, created)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        status = EXCLUDED.status,
        species = EXCLUDED.species,
        type = EXCLUDED.type,
        gender = EXCLUDED.gender,
        image = EXCLUDED.image,
        url = EXCLUDED.url,
        created = EXCLUDED.created
    """,
    values
    )

def insert_raw_characters(cur, values):
    for charcacter in values:
        cur.execute(
            """
            INSERT INTO raw.characters_raw (source_id, payload)
            VALUES (%s, %s)
            ON CONFLICT (source_id) DO UPDATE
            SET payload = EXCLUDED.payload,
                loaded_at = now();
            """,
            (
                charcacter["id"],     # source_id
                Json(charcacter),     # <-- ВАЖНО
            )
        )


def get_characters_from_raw():
    pass

conn = set_connection()
cur = conn.cursor()
characters = get_characters()


#FOR STG.characters_rm
#df_characters = set_df_characters(characters)
#characters_values = get_transform_characters(df_characters)
#insert_characters(cur, characters_values)

insert_raw_characters(cur, characters)


conn.commit()
cur.close()
conn.close()
