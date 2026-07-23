{{
    config(
        materialized='incremental',
        unique_key='order_item_sk',
    )
}}

select
    {{ dbt_utils.generate_surrogate_key([
        'p.order_id',
        'p.order_item_id',
        'p.product_id',
        'p.seller_id',
        't.customer_id',
        'p.shipping_limit_date',
        'p.price',
        'p.freight_value'
    ]) }} as order_item_sk,
    p.order_id,
    p.order_item_id,
    p.product_id,
    p.seller_id,
    t.customer_id,
    p.shipping_limit_date,
    p.price,
    p.freight_value
from {{ ref('raw_order_items') }} p
left join {{ ref('raw_orders') }} t
    on p.order_id = t.order_id

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key([
    'p.order_id',
    'p.order_item_id',
    'p.product_id',
    'p.seller_id',
    't.customer_id',
    'p.shipping_limit_date',
    'p.price',
    'p.freight_value'
]) }} not in (
    select order_item_sk from {{ this }}
)
{% endif %}