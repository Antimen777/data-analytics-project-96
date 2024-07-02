with last_click_sess as (
	select
		visitor_id,
		max(visit_date) as last_visit,
		count(visitor_id) as visit_count
	from sessions
	where medium != 'organic'
	group by 1
),

without_double as (
	select
		distinct s.visitor_id,
		s.source, 
        s.medium,
        s.campaign,
        lc.last_visit,
        lc.visit_count
	from sessions s 
	inner join last_click_sess lc on s.visitor_id = lc.visitor_id and s.visit_date = lc.last_visit
),

tab as (
    select
        wd.last_visit::date,
        wd.source,
        wd.medium,
        wd.campaign,
        sum(wd.visit_count) as visitors_count,
        count(l.lead_id) as leads_count,
        count(case when l.amount > 0 then 1 end) as purchases_count,
        sum(l.amount) as revenue
    from without_double as wd
    left join leads as l on wd.visitor_id = l.visitor_id
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
    t.source as utm_source,
    t.medium as utm_medium,
    t.campaign as utm_campaign,
    t.visitors_count,
    a.total_cost,
    t.leads_count,
    t.purchases_count,
    t.revenue
from tab as t
left join
    ads as a
    on
        t.last_visit = a.campaign_date
        and t.source = a.utm_source
        and t.medium = a.utm_medium
        and t.campaign = a.utm_campaign
order by 9 desc nulls last, 1, 5 desc, 2, 3, 4
