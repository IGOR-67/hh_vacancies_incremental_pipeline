import logging
from psycopg2.extras import execute_values

from hh.utils.path_sql import load_sql

sql_insert_ids = load_sql("staging/insert_vacancy_ids.sql")
sql_select_ids = load_sql("staging/select_vacancy_ids.sql")
sql_truncate_ids = load_sql("staging/truncate_vacancy_ids.sql")
sql_normalized = load_sql("staging/normalized.sql")


def write_ids(vacancy_ids: set[int], conn):
    """Записываем список ID в hh_vacancy_ids """
    rows = [(vid,) for vid in vacancy_ids]
    if not rows:
        logging.warning(f"List ids empty [{len(rows)}].")
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            sql_insert_ids,
            rows,
            page_size=1000
        )
    conn.commit()
    logging.info(f"List ids insert = [{len(rows)}].")


def read_ids(conn) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(sql_select_ids)
        ids = [row[0] for row in cur.fetchall()]

    logging.info("Read vacancy_ids from staging: %s", len(ids))
    return ids


def truncate_ids(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(sql_truncate_ids)
    conn.commit()


def vacancies_normalized(vacancy_ids: set[int], conn) -> None:
    with conn.cursor() as cur:
        cur.execute(sql_normalized, (vacancy_ids,))
        conn.commit()
        logging.info(f'"normalized" OK [{len(vacancy_ids)}].')