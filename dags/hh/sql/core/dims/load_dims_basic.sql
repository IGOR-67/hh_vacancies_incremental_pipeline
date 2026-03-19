-- =====================================================
-- EMPLOYER
-- =====================================================
INSERT INTO core.dim_employer (
    employer_id,
    name,
    url,
    alternate_url,
    vacancies_url,
    trusted,
    is_identified_by_esia,
    logo_urls,
    country_id,
    accredited_it,
    updated_at
)
SELECT DISTINCT ON (employer_id)
    employer_id,
    employer_name,
    employer_url,
    employer_alternate_url,
    employer_vacancies_url,
    employer_trusted,
    employer_is_identified_by_esia,
    employer_logo_urls,
    employer_country_id,
    employer_accredited_it,
    now()
FROM staging.hh_vacancies
WHERE employer_id IS NOT NULL
ORDER BY employer_id, fetched_at DESC
ON CONFLICT (employer_id) DO UPDATE
SET
    name = EXCLUDED.name,
    url = EXCLUDED.url,
    alternate_url = EXCLUDED.alternate_url,
    vacancies_url = EXCLUDED.vacancies_url,
    trusted = EXCLUDED.trusted,
    is_identified_by_esia = EXCLUDED.is_identified_by_esia,
    logo_urls = EXCLUDED.logo_urls,
    country_id = EXCLUDED.country_id,
    accredited_it = EXCLUDED.accredited_it,
    updated_at = now();

-- =====================================================
-- AREA
-- =====================================================
INSERT INTO core.dim_area (area_id, name, url, updated_at)
SELECT DISTINCT ON (area_id)
    area_id,
    area_name,
    area_url,
    now()
FROM staging.hh_vacancies
WHERE area_id IS NOT NULL
ORDER BY area_id, fetched_at DESC
ON CONFLICT (area_id) DO UPDATE
SET
    name = EXCLUDED.name,
    url = EXCLUDED.url,
    updated_at = now();

-- =====================================================
-- TYPE
-- =====================================================
INSERT INTO core.dim_type (type_id, name, updated_at)
SELECT DISTINCT ON (type_id)
    type_id,
    type_name,
    now()
FROM staging.hh_vacancies
WHERE type_id IS NOT NULL
ORDER BY type_id, fetched_at DESC
ON CONFLICT (type_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- SCHEDULE
-- =====================================================
INSERT INTO core.dim_schedule (schedule_id, name, updated_at)
SELECT DISTINCT ON (schedule_id)
    schedule_id,
    schedule_name,
    now()
FROM staging.hh_vacancies
WHERE schedule_id IS NOT NULL
ORDER BY schedule_id, fetched_at DESC
ON CONFLICT (schedule_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- EMPLOYMENT
-- =====================================================
INSERT INTO core.dim_employment (employment_id, name, updated_at)
SELECT DISTINCT ON (employment_id)
    employment_id,
    employment_name,
    now()
FROM staging.hh_vacancies
WHERE employment_id IS NOT NULL
ORDER BY employment_id, fetched_at DESC
ON CONFLICT (employment_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- EXPERIENCE
-- =====================================================
INSERT INTO core.dim_experience (experience_id, name, updated_at)
SELECT DISTINCT ON (experience_id)
    experience_id,
    experience_name,
    now()
FROM staging.hh_vacancies
WHERE experience_id IS NOT NULL
ORDER BY experience_id, fetched_at DESC
ON CONFLICT (experience_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- SALARY RANGE MODE
-- =====================================================
INSERT INTO core.dim_salary_range_mode (
    salary_range_mode_id,
    range_mode_name,
    updated_at
)
SELECT DISTINCT ON (salary_range_mode_id)
    salary_range_mode_id,
    salary_range_mode_name,
    now()
FROM staging.hh_vacancies
WHERE salary_range_mode_id IS NOT NULL
ORDER BY salary_range_mode_id, fetched_at DESC
ON CONFLICT (salary_range_mode_id) DO UPDATE
SET
    range_mode_name = EXCLUDED.range_mode_name,
    updated_at = now();

-- =====================================================
-- SALARY RANGE FREQUENCY
-- =====================================================
INSERT INTO core.dim_salary_range_frequency (
    salary_range_frequency_id,
    range_frequency_name,
    updated_at
)
SELECT DISTINCT ON (salary_range_frequency_id)
    salary_range_frequency_id,
    salary_range_frequency_name,
    now()
FROM staging.hh_vacancies
WHERE salary_range_frequency_id IS NOT NULL
ORDER BY salary_range_frequency_id, fetched_at DESC
ON CONFLICT (salary_range_frequency_id) DO UPDATE
SET
    range_frequency_name = EXCLUDED.range_frequency_name,
    updated_at = now();