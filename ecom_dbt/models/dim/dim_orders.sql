{{
    config(
        materialized='incremental',
        unique_key='order_sk',
    )
}}

select
    {{ dbt_utils.generate_surrogate_key([
        'order_id',
        'customer_id',
        'order_status',
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]) }} as order_sk,
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
from {{ ref('raw_orders') }}

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key([
    'order_id',
    'customer_id',
    'order_status',
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]) }} not in (
    select order_sk from {{ this }}
)
{% endif %}