-- ...existing code...
select
    o.order_id,
    o.customer_id,
    coalesce(p.total_amount, 0) as payment_amount,
    o.order_date,
    o.status
from {{ ref('stg_jaffle_shop__orders') }} as o
left join (
    -- aggregate payments per order to avoid multiplicative joins when there are multiple payments
    select
        orderid as order_id,
        sum(amount) as total_amount
    from {{ ref('stg_stripe__payments') }}
    group by orderid
) as p
    on o.order_id = p.order_id