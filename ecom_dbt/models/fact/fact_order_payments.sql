{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'payment_sequential']
    )
}}

select * from {{ ref('raw_order_payments') }}

{% if is_incremental() %}
where (order_id, payment_sequential) not in (
    select order_id, payment_sequential from {{ this }}
)
{% endif %}