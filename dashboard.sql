--Датасет для группировки по дате и источнику
--Группировка по последнему клику
with last_click_sess as (
    select
        visitor_id,
        max(visit_date) as last_visit
    from sessions
    where medium != 'organic'
    group by 1
),

--Убирание дубликатов, так как есть полные дубли в sessions
without_double as (
    select distinct
        s.visitor_id,
        s.source,
        lc.last_visit
    from sessions as s
    inner join
        last_click_sess as lc
        on s.visitor_id = lc.visitor_id and s.visit_date = lc.last_visit
),

--Агрегация отдельно уникальных посетителей
visitors as (
    select
        last_visit::date,
        source,
        count(visitor_id) as visitors_count
    from without_double
    group by 1, 2
),

--Агрегация всего остального с учётом логики: создание лида позже посещения
tab as (
    select
        wd.last_visit::date,
        wd.source,
        count(l.lead_id) as leads_count,
        count(case when l.amount > 0 then 1 end) as purchases_count,
        sum(l.amount) as revenue
    from without_double as wd
    left join
        leads as l
        on wd.visitor_id = l.visitor_id and wd.last_visit < l.created_at
    group by 1, 2
),

--Соединение нужных данных по рекламе
ads as (
    select
        campaign_date::date,
        utm_source,
        sum(daily_spent) as total_cost
    from vk_ads
    group by 1, 2
    union
    select
        campaign_date::date,
        utm_source,
        sum(daily_spent) as total_cost
    from ya_ads
    group by 1, 2
)

--Итоговая таблица
select
    v.visitors_count,
    a.total_cost,
    t.leads_count,
    t.purchases_count,
    t.revenue,
    coalesce(t.last_visit, a.campaign_date) as visit_date,
    coalesce(t.source, a.utm_source) as utm_source
from tab as t
inner join
    visitors as v
    on
        t.last_visit = v.last_visit
        and t.source = v.source
left join
    ads as a
    on
        t.last_visit = a.campaign_date
        and t.source = a.utm_source

-------------------------------------------------

--Тоже самое в разбивке там добавятся группировка по medium и campaign

-------------------------------------------------
