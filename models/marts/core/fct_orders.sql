with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
)

select
    orders.order_id,
    orders.customer_id,
    sum(case when payments.status = 'success' then payments.amount end) as amount
from orders
    left join payments using (order_id)
group by 1, 2