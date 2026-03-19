import datetime
import logging
import time
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

from hh.utils.staging import truncate_ids, write_ids, read_ids, vacancies_normalized
from hh.db.connect import get_conn
from hh.utils.raw import upsert_raw_vacancy_stub, update_raw_vacancy_payload, check_payload
from hh.utils.from_hh.fetch import fetch_vacancy_with_payload, fetch_vacancy_ids
from hh.config import PROFESSIONAL_ROLES
from hh.utils.core import load_dims_basic, load_dims_array, load_dims_addresses, upsert_fact_vacancies


# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id="hh_vacancies_incremental_pipeline",
    description="HH.ru vacancies ETL",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "hh_vacancies_incremental_pipeline",
        "retries": 1,
    },
    tags=["hh", "vacancies", "etl"],
) as dag:

    # ==========================================================
    # TASK 1 — DISCOVER VACANCY IDS (yesterday window)
    # ==========================================================
    @task(execution_timeout=datetime.timedelta(minutes=30))
    def task1_fetch_yesterday_vacancy_ids():
        conn = get_conn()
        # Очищаем временную таблицу с ids
        truncate_ids(conn)
        
        fetched_at = datetime.datetime.now(datetime.timezone.utc)
        context = get_current_context()
        window_start = context['data_interval_start']
        window_end = context['data_interval_end']
        
        logging.info(f"Window: {window_start} - {window_end}")
        vacancy_ids = set()

        try:
            for role_id in PROFESSIONAL_ROLES:
                page = 0
                while True:
                    data = fetch_vacancy_ids(role_id, page)
                    if data is None:
                        break
                    items = data.get("items", [])

                    for v in items:
                        pub_date = datetime.datetime.fromisoformat(
                            v["published_at"].replace("Z", "+00:00")
                        )

                        if window_start <= pub_date < window_end:
                            vacancy_id = int(v["id"])
                            vacancy_ids.add(vacancy_id)

                            # Предварительная вставка в raw с пустым JSON
                            upsert_raw_vacancy_stub(conn, vacancy_id, fetched_at, pub_date,)
                    page += 1
                    if page >= data.get("pages", 0):
                        break

                    time.sleep(0.5)

            logging.info(f"Discovered {len(vacancy_ids)} vacancies")
            write_ids(vacancy_ids, conn)

        finally:
            conn.commit()
            conn.close()

    # ==========================================================
    # TASK 2 — FETCH FULL PAYLOAD (only missing)
    # ==========================================================
    @task
    def task2_fetch_full_vacancy_payloads():
        conn = get_conn()
        vacancy_ids = read_ids(conn)
        count_error = 0
        try:
            for vacancy_id in vacancy_ids:
                if check_payload(conn, vacancy_id):
                    continue

                payload = fetch_vacancy_with_payload(vacancy_id)
                if payload is None:
                    count_error += 1

                update_raw_vacancy_payload(conn, vacancy_id, payload)

                time.sleep(0.5)
            if count_error > 0:
                logging.warning(f'"payload" download [{len(vacancy_ids) - count_error}, warning: [{count_error}]].')
            logging.info(f'"payload" download [{len(vacancy_ids) - count_error}, warning: [{count_error}]].')
        finally:
            conn.close()


    # ==========================================================
    # TASK 3 — NORMALIZE TO STAGING (only new vacancies)
    # ==========================================================
    @task
    def task3_normalize_raw_to_staging():
        """
        Извлекает нормализованные вакансии из raw.hh_vacancies
        и вставляет в staging.hh_vacancies только новые вакансии.
        """
        conn = get_conn()
        vacancy_ids = read_ids(conn)
        try:
            vacancies_normalized(vacancy_ids, conn)
        finally:
            conn.close()

    @task
    def task4_load_dims_basic():
        conn = get_conn()
        try:
            load_dims_basic(conn)
        finally:
            conn.close()

    @task
    def task5_load_dims_array():
        conn = get_conn()
        try:
            load_dims_array(conn)
        finally:
            conn.close()
            
    @task
    def task6_load_fact_vacancies():
        conn = get_conn()
        try:
            upsert_fact_vacancies(conn)            
        finally:
            conn.close()
            
    @task
    def task7_load_dim_addresses():
        conn = get_conn()
        try:
            load_dims_addresses(conn)
        finally:
            conn.close()  
    # ==========================================================
    # DAG FLOW
    # ==========================================================
    # 1️⃣ Raw → Staging
    ids = task1_fetch_yesterday_vacancy_ids()
    info = task2_fetch_full_vacancy_payloads()
    normalized = task3_normalize_raw_to_staging()
    
    # 2️⃣ Staging → Dims
    dims_basic = task4_load_dims_basic()  # employer, area, type, schedule, employment, experience, salary_range_mode/frequency
    dims_array = task5_load_dims_array()  # professional_role, skill, language и другие массивные поля
    
    # 3️⃣ Staging → Fact
    fact = task6_load_fact_vacancies()  # фактовые данные с ссылками на dim_
    
    dim_addresses = task7_load_dim_addresses()
    
    ids >> info >> normalized >> dims_basic >> dims_array >> fact >> dim_addresses
