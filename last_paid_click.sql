with tab as (
    select
        visitor_id,
        max(visit_date) as visit_date
    from sessions
    where medium != 'organic'
    group by 1
)

select
    t.visitor_id,
    t.visit_date,
    s.source as utm_source,
    s.medium as utm_medium,
    s.campaign as utm_campaign,
    l.lead_id,
    l.amount,
    l.closing_reason,
    l.status_id
from sessions as s
inner join
    tab as t
    on s.visitor_id = t.visitor_id and s.visit_date = t.visit_date
left join leads as l on s.visitor_id = l.visitor_id
order by 7 desc nulls last, 2, 3, 4, 5