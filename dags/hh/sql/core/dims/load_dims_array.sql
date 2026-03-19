-- =====================================================
-- PROFESSIONAL ROLES
-- =====================================================
INSERT INTO core.dim_professional_role (role_id, name, updated_at)
SELECT DISTINCT ON ((r->>'id')::bigint)
    (r->>'id')::bigint,
    r->>'name',
    now()
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.professional_roles) r
WHERE v.professional_roles IS NOT NULL
ON CONFLICT (role_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- BRIDGE PROFESSIONAL ROLES
-- =====================================================
INSERT INTO core.bridge_vacancy_professional_role (vacancy_id, role_id)
SELECT DISTINCT
    v.vacancy_id,
    (r->>'id')::bigint
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.professional_roles) r
WHERE v.professional_roles IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- SKILLS
-- =====================================================
INSERT INTO core.dim_skill (name, created_at)
SELECT DISTINCT
    s->>'name' AS name,
    now() AS created_at
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.key_skills) s
WHERE v.key_skills IS NOT NULL
AND s->>'name' IS NOT NULL
ON CONFLICT (name) DO NOTHING;

-- =====================================================
-- BRIDGE SKILLS
-- =====================================================
INSERT INTO core.bridge_vacancy_skill (vacancy_id, skill_name)
SELECT DISTINCT
    v.vacancy_id,
    s->>'name' AS skill_name
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.key_skills) s
WHERE v.key_skills IS NOT NULL
AND s->>'name' IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- LANGUAGES
-- =====================================================
INSERT INTO core.dim_language (
    language_id,
    name,
    level_id,
    level_name,
    updated_at
)
SELECT DISTINCT ON (l->>'id')
    l->>'id'                    AS language_id,
    l->>'name'                  AS name,
    l->'level'->>'id'           AS level_id,
    l->'level'->>'name'         AS level_name,
    now()
FROM staging.hh_vacancies v
CROSS JOIN LATERAL jsonb_array_elements(v.languages) l
WHERE v.languages IS NOT NULL
AND l->>'id' IS NOT NULL
ORDER BY l->>'id'
ON CONFLICT (language_id) DO UPDATE
SET
    name = EXCLUDED.name,
    level_id = EXCLUDED.level_id,
    level_name = EXCLUDED.level_name,
    updated_at = now();

-- =====================================================
-- BRIDGE LANGUAGES
-- =====================================================
INSERT INTO core.bridge_vacancy_language (
    vacancy_id,
    language_id
)
SELECT DISTINCT
    v.vacancy_id,
    l->>'id' AS language_id
FROM staging.hh_vacancies v
CROSS JOIN LATERAL jsonb_array_elements(v.languages) l
WHERE v.languages IS NOT NULL
AND l->>'id' IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- WORK SCHEDULE BY DAYS
-- =====================================================
INSERT INTO core.dim_work_schedule_by_days (schedule_id, name, updated_at)
SELECT DISTINCT ON (ws->>'id')
    ws->>'id',
    ws->>'name',
    now()
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.work_schedule_by_days) ws
WHERE v.work_schedule_by_days IS NOT NULL
ON CONFLICT (schedule_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- BRIDGE WORK SCHEDULE BY DAYS
-- =====================================================
INSERT INTO core.bridge_vacancy_work_schedule (vacancy_id, schedule_id)
SELECT DISTINCT
    v.vacancy_id,
    ws->>'id'
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.work_schedule_by_days) ws
WHERE v.work_schedule_by_days IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- WORKING HOURS
-- =====================================================
INSERT INTO core.dim_working_hours (hours_id, name, updated_at)
SELECT DISTINCT ON (wh->>'id')
    wh->>'id',
    wh->>'name',
    now()
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.working_hours) wh
WHERE v.working_hours IS NOT NULL
ON CONFLICT (hours_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- BRIDGE WORKING HOURS
-- =====================================================
INSERT INTO core.bridge_vacancy_working_hours (vacancy_id, hours_id)
SELECT DISTINCT
    v.vacancy_id,
    wh->>'id'
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.working_hours) wh
WHERE v.working_hours IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- WORKING FORMAT
-- =====================================================
INSERT INTO core.dim_working_format (format_id, name, updated_at)
SELECT DISTINCT ON (wf->>'id')
    wf->>'id',
    wf->>'name',
    now()
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.work_format) wf
WHERE v.work_format IS NOT NULL
ON CONFLICT (format_id) DO UPDATE
SET
    name = EXCLUDED.name,
    updated_at = now();

-- =====================================================
-- BRIDGE WORKING FORMAT
-- =====================================================
INSERT INTO core.bridge_vacancy_working_format (vacancy_id, format_id)
SELECT DISTINCT
    v.vacancy_id,
    wf->>'id'
FROM staging.hh_vacancies v,
    jsonb_array_elements(v.work_format) wf
WHERE v.work_format IS NOT NULL
ON CONFLICT DO NOTHING;