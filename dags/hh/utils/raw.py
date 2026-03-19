import json
from datetime import datetime

from hh.utils.path_sql import load_sql

sql_upsert_vacancy = load_sql("raw/upsert_vacancy.sql")
sql_update_payload = load_sql("raw/update_raw_vacancy_payload.sql")
sql_select_payload = load_sql("raw/select_payload.sql")

def upsert_raw_vacancy_stub(
    conn,
    vacancy_id: int,
    fetched_at: datetime,
    published_at: datetime,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql_upsert_vacancy,
            (
                vacancy_id,
                fetched_at,
                published_at,
                json.dumps({}),
            ),
        )

def update_raw_vacancy_payload(conn, vacancy_id: int, payload: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql_update_payload,
            (json.dumps(payload), vacancy_id),
        )
    conn.commit()
    

def check_payload(conn, vacancy_id: int):
    with conn.cursor() as cur:
        cur.execute(
            sql_select_payload,
            (vacancy_id,),
        )
        row = cur.fetchone()

    if row and row[0] and row[0] != {}:
        return True
    return False