INSERT INTO staging.hh_vacancy_ids (vacancy_id)
VALUES %s
ON CONFLICT DO NOTHING;