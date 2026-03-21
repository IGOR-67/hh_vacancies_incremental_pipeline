# mart.skill_value_analysis

## SQL

```sql
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
      AND v.salary_from > 5000
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
    CURRENT_DATE as calculated_at
FROM final_analysis
GROUP BY skill_name
HAVING COUNT(*) >= 3;

-- Индекс для быстрого поиска по навыку
CREATE INDEX idx_skill_value_name ON mart.skill_value_analysis(skill_name);
```

---

## Примеры запросов

### «Золотые навыки» — наибольшая относительная прибавка

```sql
SELECT * FROM mart.skill_value_analysis 
WHERE profit_percent > 20 
ORDER BY profit_percent DESC;
```

### Массовые и прибыльные навыки

```sql
SELECT * FROM mart.skill_value_analysis 
WHERE mentions > 50 AND clear_profit_rub > 0
ORDER BY clear_profit_rub DESC;
```

### Сравнение «соседей» (например, SQL vs NoSQL)

```sql
SELECT * FROM mart.skill_value_analysis 
WHERE skill_name IN ('sql', 'postgresql', 'mongodb')
ORDER BY clear_profit_rub DESC;
```

---

## Назначение

Таблица `mart.skill_value_analysis` — анализ рычагов капитализации специалиста.  
Показывает, какие навыки переводят вакансию из разряда «средних» в разряд «премиальных».

---

## Метрики

| Поле | Описание |
|---|---|
| `skill_name` | Нормализованный навык (приведён к нижнему регистру) |
| `mentions` | Количество вакансий с данным навыком |
| `avg_salary_with_skill` | Средняя зарплата вакансий, где навык упоминается |
| `market_baseline` | Рыночная база для сравнения (из `salary_baselines`) |
| `clear_profit_rub` | Чистая прибавка к зарплате в рублях |
| `profit_percent` | Чистая прибавка в процентах от базы |
| `calculated_at` | Дата фиксации стоимости |

---

## Группировка навыков по типу влияния

### Навыки-Множители (High Profit %)

| Навык | Пример премии |
|---|---|
| Холодные продажи | +165% |
| B2B | +89% |
| Поиск объектов | +65% |

Это «денежные» навыки, напрямую связанные с притоком выручки.  
Работодатель платит двойную-тройную цену специалисту,  
который умеет приносить деньги.

### Архитектурные и Deep-Tech навыки

| Навык | Пример премии |
|---|---|
| MLOps | +92% |
| Spring Framework | +82% |
| DevSecOps | +74% |
| Computer Vision | +43% |

Высокая прибавка обусловлена дефицитом кадров и высоким порогом входа.  
Здесь платят за то, что задачу «почти никто не умеет делать».

### Управленческие рычаги

| Навык | Пример премии |
|---|---|
| Teamleading | +60% |
| Руководство командой | +50% |
| Наставничество | +47% |

Навыки масштабирования. Компания платит премию за ответственность  
за результат целого отдела, а не одного человека.

---

## Анализ аномалий и рисков

**Низкое количество упоминаний (`mentions` 3–4):**  
Лидеры с малой выборкой (например, «Холодные продажи»: премия 220к при базе 133к)  
могут быть обусловлены единичными вакансиями топовых специалистов.  
Это «точечные» сверхприбыльные позиции, а не рыночный стандарт.

**Технологические связки:**  
`Kafka` (+30%, 7 упоминаний) — более стабильный и предсказуемый маркер «дорогой» вакансии.  
`Spring` (+82%, 3 упоминания) — мог попасть в очень узкий, дорогой сегмент выборки.

---

## Итог

| Вывод | Описание |
|---|---|
| Самый дорогой вход | DevSecOps и MLOps увеличивают стоимость почти в 2 раза |
| Эффективность данных | Power Query, ETL, Kafka стабильно дают +30–50% к базе |
| Софт-скиллы как хард-скиллы | Критическое мышление (+70%) — признак позиций Senior/Lead |

Витрина `mart.skill_value_analysis` представляет собой **рейтинг компетенций**  
по их способности увеличивать рыночную стоимость кандидата.  
Метрика `profit_percent` позволяет выделить навыки  
с наибольшим инвестиционным потенциалом,  
очищенным от влияния региональных факторов и трудового стажа.


