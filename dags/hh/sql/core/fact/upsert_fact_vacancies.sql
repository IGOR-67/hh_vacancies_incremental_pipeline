WITH upsert AS (
    INSERT INTO core.fact_vacancies (
        vacancy_id,
        name,
        fetched_at,
        created_at,
        published_at,
        initial_created_at,
        
        employer_id,
        area_id,
        type_id,
        schedule_id,
        employment_id,
        experience_id,
        salary_range_mode_id,
        salary_range_frequency_id,
        
        salary_from,
        salary_to,
        salary_currency,
        salary_gross,
        
        salary_range_from,
        salary_range_to,
        salary_range_currency,
        salary_range_gross,
        
        code,
        test,
        hidden,
        premium,
        approved,
        archived,
        contacts,
        has_test,
        relations,
        department,
        internship,
        accept_kids,
        description,
        night_shifts,
        response_url,
        alternate_url,
        negotiations_url,
        show_contacts,
        working_time_modes,
        working_time_intervals,
        working_days,
        allow_messages,
        age_restriction,
        specializations,
        accept_temporary,
        insider_interview,
        accept_handicapped,
        apply_alternate_url,
        branded_description,
        driver_license_types,
        closed_for_applicants,
        fly_in_fly_out_duration,
        response_letter_required,
        accept_incomplete_resumes,
        vacancy_constructor_template
    )
SELECT
    v.vacancy_id,
    v.name,
    v.fetched_at,
    v.created_at,
    v.published_at,
    v.initial_created_at,
    
    v.employer_id,
    v.area_id,
    v.type_id,
    v.schedule_id,
    v.employment_id,
    v.experience_id,
    
    v.salary_range_mode_id,
    v.salary_range_frequency_id,
    
    v.salary_from,
    v.salary_to,
    v.salary_currency,
    v.salary_gross,
    
    v.salary_range_from,
    v.salary_range_to,
    v.salary_range_currency,
    v.salary_range_gross,
    
    v.code,
    v.test,
    v.hidden,
    v.premium,
    v.approved,
    v.archived,
    v.contacts,
    v.has_test,
    v.relations,
    v.department,
    v.internship,
    v.accept_kids,
    regexp_replace(v.description, '<[^>]*>', '', 'g'),
    v.night_shifts,
    v.response_url,
    v.alternate_url,
    v.negotiations_url,
    v.show_contacts,
    v.working_time_modes,
    v.working_time_intervals,
    v.working_days,
    v.allow_messages,
    v.age_restriction,
    v.specializations,
    v.accept_temporary,
    v.insider_interview,
    v.accept_handicapped,
    v.apply_alternate_url,
    v.branded_description,
    v.driver_license_types,
    v.closed_for_applicants,
    v.fly_in_fly_out_duration,
    v.response_letter_required,
    v.accept_incomplete_resumes,
    v.vacancy_constructor_template
FROM staging.hh_vacancies v
ON CONFLICT (vacancy_id) 
DO UPDATE
SET
    name = EXCLUDED.name,
    fetched_at = EXCLUDED.fetched_at,
    created_at = EXCLUDED.created_at,
    published_at = EXCLUDED.published_at,
    initial_created_at = EXCLUDED.initial_created_at,
    employer_id = EXCLUDED.employer_id,
    area_id = EXCLUDED.area_id,
    type_id = EXCLUDED.type_id,
    schedule_id = EXCLUDED.schedule_id,
    employment_id = EXCLUDED.employment_id,
    experience_id = EXCLUDED.experience_id,
    salary_range_mode_id = EXCLUDED.salary_range_mode_id,
    salary_range_frequency_id = EXCLUDED.salary_range_frequency_id,
    salary_from = EXCLUDED.salary_from,
    salary_to = EXCLUDED.salary_to,
    salary_currency = EXCLUDED.salary_currency,
    salary_gross = EXCLUDED.salary_gross,
    salary_range_from = EXCLUDED.salary_range_from,
    salary_range_to = EXCLUDED.salary_range_to,
    salary_range_currency = EXCLUDED.salary_range_currency,
    salary_range_gross = EXCLUDED.salary_range_gross,
    code = EXCLUDED.code,
    test = EXCLUDED.test,
    hidden = EXCLUDED.hidden,
    premium = EXCLUDED.premium,
    approved = EXCLUDED.approved,
    archived = EXCLUDED.archived,
    contacts = EXCLUDED.contacts,
    has_test = EXCLUDED.has_test,
    relations = EXCLUDED.relations,
    department = EXCLUDED.department,
    internship = EXCLUDED.internship,
    accept_kids = EXCLUDED.accept_kids,
    description = regexp_replace(EXCLUDED.description, '<[^>]*>', '', 'g'),
    night_shifts = EXCLUDED.night_shifts,
    response_url = EXCLUDED.response_url,
    alternate_url = EXCLUDED.alternate_url,
    negotiations_url = EXCLUDED.negotiations_url,
    show_contacts = EXCLUDED.show_contacts,
    working_time_modes = EXCLUDED.working_time_modes,
    working_time_intervals = EXCLUDED.working_time_intervals,
    working_days = EXCLUDED.working_days,
    allow_messages = EXCLUDED.allow_messages,
    age_restriction = EXCLUDED.age_restriction,
    specializations = EXCLUDED.specializations,
    accept_temporary = EXCLUDED.accept_temporary,
    insider_interview = EXCLUDED.insider_interview,
    accept_handicapped = EXCLUDED.accept_handicapped,
    apply_alternate_url = EXCLUDED.apply_alternate_url,
    branded_description = EXCLUDED.branded_description,
    driver_license_types = EXCLUDED.driver_license_types,
    closed_for_applicants = EXCLUDED.closed_for_applicants,
    fly_in_fly_out_duration = EXCLUDED.fly_in_fly_out_duration,
    response_letter_required = EXCLUDED.response_letter_required,
    accept_incomplete_resumes = EXCLUDED.accept_incomplete_resumes,
    vacancy_constructor_template = EXCLUDED.vacancy_constructor_template,
    updated_at = now()
WHERE 
    fact_vacancies.name        IS DISTINCT FROM EXCLUDED.name
    OR fact_vacancies.fetched_at        IS DISTINCT FROM EXCLUDED.fetched_at
    OR fact_vacancies.created_at        IS DISTINCT FROM EXCLUDED.created_at
    OR fact_vacancies.published_at        IS DISTINCT FROM EXCLUDED.published_at
    OR fact_vacancies.initial_created_at        IS DISTINCT FROM EXCLUDED.initial_created_at
    OR fact_vacancies.employer_id        IS DISTINCT FROM EXCLUDED.employer_id
    OR fact_vacancies.area_id        IS DISTINCT FROM EXCLUDED.area_id
    OR fact_vacancies.type_id        IS DISTINCT FROM EXCLUDED.type_id
    OR fact_vacancies.schedule_id        IS DISTINCT FROM EXCLUDED.schedule_id
    OR fact_vacancies.employment_id        IS DISTINCT FROM EXCLUDED.employment_id
    OR fact_vacancies.experience_id        IS DISTINCT FROM EXCLUDED.experience_id
    OR fact_vacancies.salary_range_mode_id        IS DISTINCT FROM EXCLUDED.salary_range_mode_id    
    OR fact_vacancies.salary_range_frequency_id        IS DISTINCT FROM EXCLUDED.salary_range_frequency_id
    OR fact_vacancies.salary_from        IS DISTINCT FROM EXCLUDED.salary_from
    OR fact_vacancies.salary_to        IS DISTINCT FROM EXCLUDED.salary_to
    OR fact_vacancies.salary_currency        IS DISTINCT FROM EXCLUDED.salary_currency
    OR fact_vacancies.salary_gross        IS DISTINCT FROM EXCLUDED.salary_gross
    OR fact_vacancies.salary_range_from        IS DISTINCT FROM EXCLUDED.salary_range_from
    OR fact_vacancies.salary_range_to        IS DISTINCT FROM EXCLUDED.salary_range_to
    OR fact_vacancies.salary_range_currency        IS DISTINCT FROM EXCLUDED.salary_range_currency
    OR fact_vacancies.salary_range_gross        IS DISTINCT FROM EXCLUDED.salary_range_gross
    OR fact_vacancies.code        IS DISTINCT FROM EXCLUDED.code
    OR fact_vacancies.test        IS DISTINCT FROM EXCLUDED.test
    OR fact_vacancies.hidden        IS DISTINCT FROM EXCLUDED.hidden
    OR fact_vacancies.premium        IS DISTINCT FROM EXCLUDED.premium
    OR fact_vacancies.approved        IS DISTINCT FROM EXCLUDED.approved
    OR fact_vacancies.archived        IS DISTINCT FROM EXCLUDED.archived
    OR fact_vacancies.contacts        IS DISTINCT FROM EXCLUDED.contacts
    OR fact_vacancies.has_test        IS DISTINCT FROM EXCLUDED.has_test
    OR fact_vacancies.relations        IS DISTINCT FROM EXCLUDED.relations
    OR fact_vacancies.department        IS DISTINCT FROM EXCLUDED.department
    OR fact_vacancies.internship        IS DISTINCT FROM EXCLUDED.internship
    OR fact_vacancies.accept_kids        IS DISTINCT FROM EXCLUDED.accept_kids
    OR fact_vacancies.description        IS DISTINCT FROM EXCLUDED.description
    OR fact_vacancies.night_shifts        IS DISTINCT FROM EXCLUDED.night_shifts
    OR fact_vacancies.response_url        IS DISTINCT FROM EXCLUDED.response_url
    OR fact_vacancies.alternate_url        IS DISTINCT FROM EXCLUDED.alternate_url
    OR fact_vacancies.negotiations_url        IS DISTINCT FROM EXCLUDED.negotiations_url
    OR fact_vacancies.show_contacts        IS DISTINCT FROM EXCLUDED.show_contacts
    OR fact_vacancies.working_time_modes        IS DISTINCT FROM EXCLUDED.working_time_modes
    OR fact_vacancies.working_time_intervals        IS DISTINCT FROM EXCLUDED.working_time_intervals
    OR fact_vacancies.working_days        IS DISTINCT FROM EXCLUDED.working_days
    OR fact_vacancies.allow_messages        IS DISTINCT FROM EXCLUDED.allow_messages
    OR fact_vacancies.age_restriction        IS DISTINCT FROM EXCLUDED.age_restriction
    OR fact_vacancies.specializations        IS DISTINCT FROM EXCLUDED.specializations
    OR fact_vacancies.accept_temporary        IS DISTINCT FROM EXCLUDED.accept_temporary
    OR fact_vacancies.insider_interview        IS DISTINCT FROM EXCLUDED.insider_interview
    OR fact_vacancies.accept_handicapped        IS DISTINCT FROM EXCLUDED.accept_handicapped
    OR fact_vacancies.apply_alternate_url        IS DISTINCT FROM EXCLUDED.apply_alternate_url
    OR fact_vacancies.branded_description        IS DISTINCT FROM EXCLUDED.branded_description
    OR fact_vacancies.driver_license_types        IS DISTINCT FROM EXCLUDED.driver_license_types
    OR fact_vacancies.closed_for_applicants        IS DISTINCT FROM EXCLUDED.closed_for_applicants
    OR fact_vacancies.fly_in_fly_out_duration        IS DISTINCT FROM EXCLUDED.fly_in_fly_out_duration
    OR fact_vacancies.response_letter_required        IS DISTINCT FROM EXCLUDED.response_letter_required
    OR fact_vacancies.accept_incomplete_resumes        IS DISTINCT FROM EXCLUDED.accept_incomplete_resumes
    OR fact_vacancies.vacancy_constructor_template        IS DISTINCT FROM EXCLUDED.vacancy_constructor_template
RETURNING xmax
)
SELECT
COUNT(*) FILTER (WHERE xmax = 0) AS inserted,
COUNT(*) FILTER (WHERE xmax <> 0) AS updated
FROM upsert;