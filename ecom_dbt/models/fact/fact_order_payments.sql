{{
    config(
        materialized='incremental',
        unique_key='order_payment_sk',
    )
}}

select
    {{ dbt_utils.generate_surrogate_key([
        'order_id',
        'payment_sequential',
        'payment_type',
        'payment_installments',
        'payment_value'
    ]) }} as order_payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from {{ ref('raw_order_payments') }}

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key([
    'order_id',
    'payment_sequential',
    'payment_type',
    'payment_installments',
    'payment_value'
]) }} not in (
    select order_payment_sk from {{ this }}
)
{% endif %}