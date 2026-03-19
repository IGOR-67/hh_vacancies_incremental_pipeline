SELECT vacancy_id, address
FROM staging.hh_vacancies
WHERE address IS NOT NULL;