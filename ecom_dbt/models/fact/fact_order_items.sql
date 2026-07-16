{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id']
    )
}}

select * from {{ ref('raw_order_items') }}

{% if is_incremental() %}
where (order_id, order_item_id) not in (
    select order_id, order_item_id from {{ this }}
)
{% endif %}