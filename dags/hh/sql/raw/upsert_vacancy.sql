INSERT INTO raw.hh_vacancies (vacancy_id, fetched_at, published_at, payload)
VALUES (%s, %s, %s, %s)
ON CONFLICT (vacancy_id) DO UPDATE
SET
    fetched_at = EXCLUDED.fetched_at,
    published_at = EXCLUDED.published_at;