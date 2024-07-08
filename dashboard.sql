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
    t.last_visit as visit_date,
    v.visitors_count,
    t.source as utm_source,
    t.medium as utm_medium,
    t.campaign as utm_campaign,
    a.total_cost,
    t.leads_count,
    t.purchases_count,
    t.revenue
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

--Тоже самое только добавляется группировка по medium и campaign
with last_click_sess as (
    select
        visitor_id,
        max(visit_date) as last_visit
    from sessions
    where medium != 'organic'
    group by 1
),

without_double as (
    select distinct
        s.visitor_id,
        s.source,
        s.medium,
        s.campaign,
        lc.last_visit
    from sessions as s
    inner join
        last_click_sess as lc
        on s.visitor_id = lc.visitor_id and s.visit_date = lc.last_visit
),

visitors as (
    select
        last_visit::date,
        source,
        medium,
        campaign,
        count(visitor_id) as visitors_count
    from without_double
    group by 1, 2, 3, 4
),

tab as (
    select
        wd.last_visit::date,
        wd.source,
        wd.medium,
        wd.campaign,
        count(l.lead_id) as leads_count,
        count(case when l.amount > 0 then 1 end) as purchases_count,
        sum(l.amount) as revenue
    from without_double as wd
    left join
        leads as l
        on wd.visitor_id = l.visitor_id and wd.last_visit < l.created_at
    group by 1, 2, 3, 4
),

ads as (
    select
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from vk_ads
    group by 1, 2, 3, 4
    union
    select
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from ya_ads
    group by 1, 2, 3, 4
)

select
    t.last_visit as visit_date,
    v.visitors_count,
    t.source as utm_source,
    t.medium as utm_medium,
    t.campaign as utm_campaign,
    a.total_cost,
    t.leads_count,
    t.purchases_count,
    t.revenue
from tab as t
inner join
    visitors as v
    on
        t.last_visit = v.last_visit
        and t.source = v.source
        and t.medium = v.medium
        and t.campaign = v.campaign
left join
    ads as a
    on
        t.last_visit = a.campaign_date
        and t.source = a.utm_source
        and t.medium = a.utm_medium
        and t.campaign = a.utm_campaign

-------------------------------------------------

--Датасет где не требуется данные по затратам на рекламу
with tab as (
    select distinct *
    from sessions
)

select
    t.visitor_id,
    t.visit_date::date,
    t.source,
    t.medium,
    t.campaign,
    l.lead_id,
    l.amount,
    l.created_at::date,
    l.status_id
from tab as t
left join leads as l on t.visitor_id = l.visitor_id

--Формула в Datalens для конверсии в лидов
COUNTD([lead_id]) / COUNT([visitor_id]) * 100
--Формула в Datalens для конверсии в оплату
COUNTD_IF([lead_id], [amount] > 0) / COUNTD([lead_id]) * 100

-------------------------------------------------

--Датасет для расчёта закрытия 90% лидов
with last_click_sess as (
    select
        visitor_id,
        max(visit_date) as last_visit
    from sessions
    group by 1
),

without_double as (
    select distinct
        s.visitor_id,
        lc.last_visit
    from sessions as s
    inner join
        last_click_sess as lc
        on s.visitor_id = lc.visitor_id and s.visit_date = lc.last_visit
)

select
    wd.visitor_id,
    wd.last_visit,
    l.created_at,
    ntile(10) over (order by l.created_at - wd.last_visit)
from without_double as wd
inner join leads as l on wd.visitor_id = l.visitor_id
where l.created_at > wd.last_visit

--Формула в Datalens для вывода значения дней с фильтром на 9 ntile.
ROUND(CEILING(MAX([created_at] - [last_visit])))
