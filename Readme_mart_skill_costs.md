# mart.skill_costs

## SQL

```sql
CREATE TABLE mart.skill_costs AS
WITH cleansed_data AS (
    SELECT 
        vacancy_id, area_id, experience_id,
        CASE 
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from::numeric + salary_to::numeric) / 2
            WHEN salary_from IS NOT NULL THEN salary_from::numeric
            WHEN salary_to IS NOT NULL THEN salary_to::numeric
            ELSE NULL 
        END AS avg_salary
    FROM core.fact_vacancies
    WHERE (salary_currency = 'RUR' OR salary_currency IS NULL)
      AND (salary_from IS NOT NULL OR salary_to IS NOT NULL)
),
market_avg AS (
    SELECT area_id, experience_id, AVG(avg_salary) as segment_avg_salary
    FROM cleansed_data
    GROUP BY area_id, experience_id
)
SELECT 
    LOWER(bridge.skill_name) as skill_name,
    COUNT(*) as count_vacancies,
    CAST(AVG(v.avg_salary) AS BIGINT) as avg_salary_total,
    CAST(AVG(v.avg_salary - m.segment_avg_salary) AS BIGINT) as skill_value_premium,
    CURRENT_DATE as calculation_date
FROM cleansed_data v
JOIN core.bridge_vacancy_skill bridge ON v.vacancy_id = bridge.vacancy_id
JOIN market_avg m ON v.area_id = m.area_id AND v.experience_id = m.experience_id
GROUP BY 1
HAVING COUNT(*) >= 3;
```

---

## Назначение

Таблица `mart.skill_costs` — итоговая аналитическая витрина ценности компетенций.  
В отличие от базовых стоимостей (которые описывают рынок «в общем»),  
эта таблица описывает конкретные рычаги влияния на доход.

Она отвечает на вопрос:

> «Сколько рынок готов доплачивать сверху за владение конкретным инструментом или знанием?»

Витрина переводит абстрактные требования вакансий в конкретные денежные эквиваленты.

---

## Метрики

| Поле | Описание |
|---|---|
| `skill_name` | Нормализованный навык (приведён к нижнему регистру через `LOWER`) |
| `count_vacancies` | Популярность навыка — количество вакансий, где он упоминается |
| `avg_salary_total` | Средняя зарплата вакансий, где встречается навык |
| `skill_value_premium` | Чистая стоимость навыка — премия сверх рыночной базы |
| `calculation_date` | Дата расчёта |

### count_vacancies

| Значение | Вывод |
|---|---|
| Высокий + высокая премия | «Золотой стандарт» индустрии |
| Низкий + высокая премия | Узкая дефицитная ниша |

### avg_salary_total

Может вводить в заблуждение: в Москве оно всегда выше.  
Поэтому основной метрикой является `skill_value_premium`.

### skill_value_premium

**Самая важная метрика.**  
Разница между реальной зарплатой в вакансии и «базой» для данного города и опыта.

| Значение | Вывод |
|---|---|
| Положительное | Навык увеличивает доход выше рыночной нормы |
| Отрицательное | Навык-маркер низкооплачиваемых должностей |

---

## Методология

Расчёт строится на сравнении внутри сегмента:

1. Берём вакансию с навыком (например, `Python` в Новосибирске, опыт 1–3 года)
2. Вычитаем среднюю зарплату всех вакансий в Новосибирске с опытом 1–3 года
3. Полученный остаток усредняем по всем упоминаниям навыка

Это позволяет честно оценить навык,  
не смешивая «дорогие» города с «дешёвыми» и «сеньоров» с «джунами».

---

## Примеры

| Навык | Вакансий | Премия | Вывод |
|---|---|---|---|
| `sql` | 195 | +16 263 ₽ | Массовый навык, даёт умеренную, но стабильную прибавку |
| `mlops` | 3 | +139 729 ₽ | Дефицитный навык, рынок «штучный» |
| `выкладка товаров` | 263 | −12 281 ₽ | Навык-маркер низкооплачиваемых должностей |

---

## Итог

Витрина `mart.skill_costs` представляет собой **рейтинг компетенций**,  
ранжированных по их способности увеличивать рыночную стоимость кандидата.  
Метрика `skill_value_premium` позволяет выделить навыки  
с наибольшим инвестиционным потенциалом для обучения,  
так как она очищена от влияния региональных факторов и трудового стажа.


