{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

select * from {{ ref('raw_orders') }}

{% if is_incremental() %}
where order_id not in (select order_id from {{ this }})
{% endif %}