with orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),

payments as (
    select order_id,
        sum (case when  payment_status = 'success' then payment_amount end) as amount
        from {{  ref('stg_stripe__payments') }}
    group by 1
),


 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        coalesce (payments.amount, 0) as amount

    from orders
    left join payments on orders.order_id = payments.order_id
)

select * from final