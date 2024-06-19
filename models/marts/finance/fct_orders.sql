with orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),

payments as (
    select order_id,
        sum (case when status = 'success' then amount end) as amount
        from {{  ref('stg_stripe__payments') }}
    group by 1
),


 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (payments.amount, 0) as amount

    from orders
    left join payments using (order_id)
)

select * from final