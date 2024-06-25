-- with statement
with

-- import CTEs

customers as (
    select * from 
       {{ ref('stg_jaffle_shop__customers') }}

),

paid_orders as (
        select * from {{ ref('int_orders') }} 
),

-- final CTE
final as (
    select
            paid_orders.order_id,
            paid_orders.customer_id,
           paid_orders. order_placed_at,
            paid_orders.order_status,
            paid_orders.total_amount_paid,
            paid_orders.payment_finalized_date,
            customerscustomer_first_name,
            customers.customer_last_name,
            
            row_number() over (order by paid_orders.order_placed_at, paid_orders.order_id) as transaction_seq,
            row_number() over (partition by paid_orders.customer_id order by paid_orders.order_placed_at, paid_orders.order_id) as customer_sales_seq,
            
            case 
            when (
                rank() over(
                    partition by paid_orders.customer_id
                    order by paid_orders.order_placed_at, paid_orders.order_id
                ) = 1
            ) then 'new'
            else 'return' end as nvsr,

            -- customer lifetime value
            sum(paid_orders.total_amount_paid) over(
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) as customer_lifetime_value,

            -- first day of sale
            first_value(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) as fdos
        from paid_orders 
        left join customers on paid_orders.customer_id = customers.customer_id
        

)
-- Simple select statement
select * from final
