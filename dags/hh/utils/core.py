import logging
from psycopg2.extras import execute_values

from hh.utils.path_sql import load_sql


sql_insert_dims_basic = load_sql("core/dims/load_dims_basic.sql")
sql_insert_dims_array = load_sql("core/dims/load_dims_array.sql")
sql_load_addresses = load_sql("core/dims/load_dims_addresses.sql")
sql_upsert_vacancy = load_sql("core/fact/upsert_fact_vacancies.sql")
sql_select_vacancies_address = load_sql("staging/select_vacancies_address.sql")

def load_dims_basic(conn):
    with conn.cursor() as cur:
        cur.execute(sql_insert_dims_basic)

    conn.commit()
    logging.info('"load_dims_basic" OK.')

def load_dims_array(conn):
    with conn.cursor() as cur:
        cur.execute(sql_insert_dims_array)

    conn.commit()
    logging.info('"load_dims_array" OK.')

def load_dims_addresses(conn) -> None:
    address_values = []
    metro_values = []

    with conn.cursor() as cur:
        cur.execute(sql_select_vacancies_address)
        rows = cur.fetchall()

        for vacancy_id, address in rows:
            if not address:
                continue

            address_values.append((
                vacancy_id,
                address.get("raw"),
                address.get("city"),
                address.get("street"),
                address.get("building"),
                address.get("description"),
                address.get("lat"),
                address.get("lng"),
            ))

            for m in address.get("metro_stations", []):
                metro_values.append((
                    vacancy_id,
                    m.get("station_id"),
                    m.get("station_name"),
                    m.get("line_id"),
                    m.get("line_name"),
                    m.get("lat"),
                    m.get("lng"),
                ))

        if address_values:
            execute_values(
                cur,
                sql_load_addresses.split(";")[0] + ";",
                address_values,
                page_size=1000,
            )

        if metro_values:
            execute_values(
                cur,
                sql_load_addresses.split(";")[1] + ";",
                metro_values,
                page_size=1000,
            )

    conn.commit()

    logging.info(
        '"load_vacancy_addresses" OK. addresses=%s metro=%s',
        len(address_values),
        len(metro_values),
    )

def upsert_fact_vacancies(conn):
    with conn.cursor() as cur:
        cur.execute(sql_upsert_vacancy)
        inserted, updated = cur.fetchone()
        
    conn.commit()
    logging.info(f'Loaded to fact: inserted={inserted}, updated={updated}')
    return inserted, updated