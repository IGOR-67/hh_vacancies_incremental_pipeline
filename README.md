# HH Vacancies Incremental Pipeline

## 1. О проекте

Настоящий репозиторий содержит реализацию инкрементального ETL-конвейера для автоматизированного сбора, обработки и хранения данных о вакансиях в сфере информационных технологий, публикуемых на платформе HH.ru.

Конвейер реализован в рамках выпускной квалификационной работы магистра по теме «Разработка сервиса извлечения структурированной информации из текстов IT-вакансий на основе методов обработки естественного языка и машинного обучения».

Цель конвейера — автоматизированное ежедневное получение вакансий через публичный API HH.ru, их последовательная трансформация через слои raw → staging → core, а также подготовка данных для последующего анализа в Power BI и применения методов обработки естественного языка (NLP) для извлечения структурированной информации о требованиях к кандидатам.
## 2. Стек технологий

| Компонент | Технология |
|---|---|
| Оркестрация конвейера | Apache Airflow 2.9.1 |
| Хранилище данных | PostgreSQL 15 |
| Контейнеризация | Docker / Docker Compose |
| Язык реализации | Python 3.12 |
| Драйвер БД | psycopg2 |
| Источник данных | HH.ru Public API |
| Визуализация | Power BI |
## 3. Архитектура

### 3.1 Общая схема

Конвейер реализует инкрементальную загрузку: на каждом суточном запуске обрабатываются исключительно вакансии, опубликованные в пределах временного окна data_interval_start — data_interval_end.
### 3.2 DAG: hh_vacancies_incremental_pipeline

| Параметр | Значение |
|---|---|
| Расписание | @daily |
| catchup | False |
| max_active_runs | 1 |
| Временное окно | data_interval_start — data_interval_end |

Порядок выполнения задач:
task1_fetch_yesterday_vacancy_ids │ ▼ task2_fetch_full_vacancy_payloads │ ▼ task3_normalize_raw_to_staging │ ▼ task4_load_dims_basic │ ▼ task5_load_dims_array │ ▼ task6_load_fact_vacancies │ ▼ task7_load_dim_addresses


| Задача | Описание |
|---|---|
| task1_fetch_yesterday_vacancy_ids | Запрашивает идентификаторы вакансий через API HH.ru по 26 профессиональным ролям IT-направления; фильтрует по дате публикации; вставляет заглушки в raw.hh_vacancies; записывает идентификаторы в staging.hh_vacancy_ids |
| task2_fetch_full_vacancy_payloads | Для каждого идентификатора, у которого отсутствует полный JSON-payload, выполняет запрос к API HH.ru и сохраняет результат в raw.hh_vacancies |
| task3_normalize_raw_to_staging | Извлекает и нормализует поля из JSON-payload; записывает результат в staging.hh_vacancies |
| task4_load_dims_basic | Загружает справочники: dim_employer, dim_area, dim_type, dim_schedule, dim_employment, dim_experience, dim_salary_range_mode, dim_salary_range_frequency |
| task5_load_dims_array | Загружает справочники массивных полей и таблицы-мосты: dim_professional_role, dim_skill, dim_language, dim_work_schedule_by_days, dim_working_hours, dim_working_format и соответствующие bridge_* |
| task6_load_fact_vacancies | Выполняет upsert фактовых данных в core.fact_vacancies; HTML-теги в поле description удаляются посредством регулярного выражения; фиксирует количество вставленных и обновлённых записей |
| task7_load_dim_addresses | Загружает адресные данные в core.dim_vacancy_addresses и core.dim_vacancy_metro_stations |

## 4. Слои данных

### Raw

Схема raw. Содержит первичные данные, полученные непосредственно из API HH.ru.

| Таблица | Описание |
|---|---|
| raw.hh_vacancies | Полный JSON-payload каждой вакансии (payload), метки времени получения (fetched_at) и публикации (published_at) |

### Staging

Схема staging. Содержит нормализованные данные, извлечённые из JSON-payload.

| Таблица | Описание |
|---|---|
| staging.hh_vacancy_ids | Временная таблица идентификаторов вакансий текущего инкремента |
| staging.hh_vacancies | Нормализованные поля вакансий (более 70 атрибутов), включая вложенные JSONB-структуры |

### Core

Схема core. Реализует модель «звезда» (Star Schema).

| Группа | Таблицы |
|---|---|
| Факты | fact_vacancies |
| Справочники (скалярные поля) | dim_employer, dim_area, dim_type, dim_schedule, dim_employment, dim_experience, dim_salary_range_mode, dim_salary_range_frequency |
| Справочники (массивные поля) | dim_professional_role, dim_skill, dim_language, dim_work_schedule_by_days, dim_working_hours, dim_working_format |
| Таблицы-мосты | bridge_vacancy_professional_role, bridge_vacancy_skill, bridge_vacancy_language, bridge_vacancy_work_schedule, bridge_vacancy_working_hours, bridge_vacancy_working_format |
| Адресные таблицы | dim_vacancy_addresses, dim_vacancy_metro_stations |

### Mart

Схема mart. Содержит аналитические витрины и CSV-выгрузки, предназначенные для подключения Power BI и применения NLP-методов.

## 5. Структура репозитория

```
.
├── dags/
│   ├── hh/
│   │   ├── config.py                   # Базовый URL API, коды профессиональных ролей, HTTP-заголовки
│   │   ├── hh_pipeline_dag.py          # Определение DAG и всех задач конвейера
│   │   ├── db/
│   │   │   └── connect.py              # Функция get_conn() — подключение к PostgreSQL через psycopg2
│   │   ├── sql/
│   │   │   ├── raw/
│   │   │   │   ├── upsert_vacancy.sql              # Вставка/обновление заглушки вакансии в raw
│   │   │   │   ├── update_raw_vacancy_payload.sql  # Запись полного JSON-payload
│   │   │   │   └── select_payload.sql              # Проверка наличия payload
│   │   │   ├── staging/
│   │   │   │   ├── insert_vacancy_ids.sql          # Вставка ID в staging.hh_vacancy_ids
│   │   │   │   ├── select_vacancy_ids.sql          # Чтение ID из staging.hh_vacancy_ids
│   │   │   │   ├── truncate_vacancy_ids.sql        # Очистка staging.hh_vacancy_ids
│   │   │   │   ├── normalized.sql                  # Нормализация raw → staging
│   │   │   │   └── select_vacancies_address.sql    # Чтение адресов для загрузки в core
│   │   │   └── core/
│   │   │       ├── dims/
│   │   │       │   ├── load_dims_basic.sql         # Загрузка скалярных справочников
│   │   │       │   ├── load_dims_array.sql         # Загрузка массивных справочников и bridge-таблиц
│   │   │       │   └── load_dims_addresses.sql     # Загрузка адресных таблиц
│   │   │       └── fact/
│   │   │           └── upsert_fact_vacancies.sql   # Upsert фактовой таблицы
│   │   └── utils/
│   │       ├── core.py                 # Функции загрузки справочников и фактов в core
│   │       ├── raw.py                  # Функции работы со слоем raw
│   │       ├── staging.py              # Функции работы со слоем staging
│   │       ├── path_sql.py             # Вспомогательная функция загрузки SQL-файлов
│   │       └── from_hh/
│   │           └── fetch.py            # HTTP-запросы к API HH.ru
├── docker-compose.yml                  # Описание сервисов Docker
├── .env                                # Переменные окружения (не включается в репозиторий)
├── logs/                               # Логи выполнения задач Airflow
└── plugins/                            # Плагины Airflow (при наличии)
```
## 6. Окружение (.env)

### 6.1 Назначение переменных

Файл .env содержит три группы переменных:

1. Учётные данные базы данных метаданных Airflow (airflow-db) — используются сервисами airflow-init, airflow-webserver, airflow-scheduler.
2. Строка подключения Airflow к собственной БД (SQL_ALCHEMY_CONN) — передаётся в конфигурацию Airflow.
3. Параметры подключения DAG-скриптов к рабочей БД проекта (hh-db) — используются модулем dags/hh/db/connect.py.
### 6.2 Пример файла .env

```
# ── Учётные данные БД метаданных Airflow ──────────────────────────────────────
POSTGRES_USER=*****
POSTGRES_PASSWORD=*****
POSTGRES_DB=airflow

# ── Конфигурация Airflow ───────────────────────────────────────────────────────
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__BASE_LOG_FOLDER=/opt/airflow/logs

# ── Подключение Airflow к собственной БД ──────────────────────────────────────
SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow

# ── Подключение DAG-скриптов к рабочей БД проекта ─────────────────────────────
DB_HOST=hh-db
DB_NAME=hh_db
DB_USER=*****
DB_PASSWORD=*****
DB_PORT=5432
```

Внимание. Файл .env содержит конфиденциальные учётные данные и не должен включаться в систему контроля версий. Добавьте .env в .gitignore

## 7. Развёртывание в Docker

### 7.1 Требования

- Docker (версия 24.0 и выше)
- Docker Compose (версия 2.0 и выше)

### 7.2 Сервисы docker-compose.yml

| Сервис | Образ | Описание |
|---|---|---|
| airflow-db | postgres:15 | База данных метаданных Airflow; оснащена healthcheck-проверкой готовности |
| airflow-init | apache/airflow:2.9.1 | Инициализация БД Airflow (airflow db init) и создание пользователя admin |
| airflow-webserver | apache/airflow:2.9.1 | Веб-интерфейс Airflow; доступен на порту 8080 |
| airflow-scheduler | apache/airflow:2.9.1 | Планировщик выполнения DAG-задач |
| hh-db | postgres:15 | Рабочая база данных проекта; доступна с хоста на порту 5400 |

Тома (volumes):

| Том | Назначение |
|---|---|
| airflow_pgdata_new | Персистентное хранилище данных airflow-db |
| hh_db_data | Персистентное хранилище данных hh-db |

Сеть: все сервисы объединены в сеть airflow_network (драйвер bridge).
