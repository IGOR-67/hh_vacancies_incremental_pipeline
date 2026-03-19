-- 2. Создаем новую витрину
CREATE TABLE mart.skill_value_analysis AS
WITH skill_data AS (
    SELECT 
        LOWER(s.skill_name) as skill_name,
        v.area_id,
        v.experience_id,
        CASE 
            WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from::numeric + v.salary_to::numeric) / 2
            WHEN v.salary_from IS NOT NULL THEN v.salary_from::numeric
            WHEN v.salary_to IS NOT NULL THEN v.salary_to::numeric
        END AS vacancy_salary
    FROM core.fact_vacancies v
    JOIN core.bridge_vacancy_skill s ON v.vacancy_id = s.vacancy_id
    WHERE (v.salary_currency = 'RUR' OR v.salary_currency IS NULL)
      AND (v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL)
      AND v.salary_from > 5000 -- Исключаем подозрительно низкие зарплаты
),
final_analysis AS (
    SELECT 
        sd.skill_name,
        sd.vacancy_salary,
        b.base_avg_salary,
        (sd.vacancy_salary - b.base_avg_salary) as money_diff
    FROM skill_data sd
    JOIN mart.salary_baselines b ON sd.area_id = b.area_id 
                               AND sd.experience_id = b.experience_id
)
SELECT 
    skill_name,
    COUNT(*) as mentions,
    ROUND(AVG(vacancy_salary))::BIGINT as avg_salary_with_skill,
    ROUND(AVG(base_avg_salary))::BIGINT as market_baseline,
    ROUND(AVG(money_diff))::BIGINT as clear_profit_rub,
    ROUND(AVG(money_diff) / NULLIF(AVG(base_avg_salary), 0) * 100, 1) as profit_percent,
    CURRENT_DATE as calculated_at -- Дата фиксации стоимости
FROM final_analysis
GROUP BY skill_name
HAVING COUNT(*) >= 3;

-- 3. Добавляем индекс для быстрого поиска по навыку
CREATE INDEX idx_skill_value_name ON mart.skill_value_analysis(skill_name);

-- 1. Найти "Золотые навыки" (Самая высокая относительная прибавка):
SELECT * FROM mart.skill_value_analysis 
WHERE profit_percent > 20 
ORDER BY profit_percent DESC;

-- 2. Найти самые массовые, но прибыльные навыки:
SELECT * FROM mart.skill_value_analysis 
WHERE mentions > 50 AND clear_profit_rub > 0
ORDER BY clear_profit_rub DESC;

-- 3. Сравнить "соседей" (Например, SQL vs NoSQL):
SELECT * FROM mart.skill_value_analysis 
WHERE skill_name IN ('sql', 'postgresql', 'mongodb')
ORDER BY clear_profit_rub DESC;




Интерпретация этой таблицы — это анализ рычагов капитализации специалиста. Ты получил данные о том, какие навыки переводят вакансию из разряда «средних» в разряд «премиальных».
Вот как правильно описать эти результаты (например, в отчете или статье):
1. Группировка навыков по «типу влияния»
Навыки-Множители (High Profit %):
Пример: Холодные продажи (+165%), B2B (+89%), Поиск объектов (+65%).
Интерпретация: Это «денежные» навыки. Они напрямую связаны с притоком выручки в компанию. Работодатель готов платить двойную-тройную цену (относительно базы) специалисту, который умеет приносить деньги.
Архитектурные и Deep-Tech навыки:
Пример: MLOps (+92%), DevSecOps (+74%), Spring Framework (+82%), Computer Vision (+43%).
Интерпретация: Это навыки сложности. Высокая прибавка обусловлена дефицитом кадров и высоким порогом входа. Здесь платят за то, что задачу «почти никто не умеет делать».
Управленческие рычаги:
Пример: Teamleading (+60%), Руководство командой (+50%), Наставничество (+47%).
Интерпретация: Навыки масштабирования. Компания платит премию за ответственность за результат целого отдела, а не одного человека.
2. Анализ аномалий и рисков
Низкое количество упоминаний (Mentions 3-4): Важно сделать оговорку, что такие лидеры, как «Холодные продажи» (премия 220к при базе 133к), могут быть обусловлены 3-мя вакансиями топовых сейлзов в нефтянке или IT. Это «точечные» сверхприбыльные позиции.
Технологические связки: Обрати внимание, что Kafka (+30%) имеет 7 упоминаний, а Spring (+82%) всего 3. Это значит, что Kafka — более стабильный и предсказуемый маркер «дорогой» вакансии, а Spring в данной выборке мог попасть в очень узкий, дорогой сегмент.
3. Резюме для интерпретации (Key Takeaways)
Самый дорогой вход: Технологии автоматизации и безопасности (DevSecOps, MLOps) увеличивают стоимость почти в 2 раза.
Эффективность данных: Инструменты обработки данных (Power Query, ETL, Kafka) стабильно дают профит +30–50% к базе.
Софт-скиллы как хард-скиллы: Критическое мышление (+70%) в вакансиях с высокой базой (126к) — это признак позиций уровня Senior/Lead, где за логику и принятие решений платят почти как за код.