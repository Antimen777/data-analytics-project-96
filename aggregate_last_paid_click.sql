with tab as (
    select
        s.visit_date::date,
        s.source,
        s.medium,
        s.campaign,
        count(s.visitor_id) as visitors_count,
        count(l.lead_id) as leads_count,
        count(case when status_id = 142 then 1 end) as purchases_count,
        sum(l.amount) as revenue
    from sessions as s
    left join leads as l on s.visitor_id = l.visitor_id
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
    t.visit_date,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    t.visitors_count,
    a.total_cost,
    t.leads_count,
    t.purchases_count,
    t.revenue
from tab as t
inner join
    ads as a
    on
        t.visit_date = a.campaign_date
        and t.source = a.utm_source
        and t.medium = a.utm_medium
        and t.campaign = a.utm_campaign
order by 9 desc nulls last, 1, 5 desc, 2, 3, 4