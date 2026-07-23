{{
    config(
        materialized='incremental',
        unique_key='order_review_sk'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key([
        'review_id',
        'order_id',
        'review_score',
        'review_creation_date',
        'review_answer_timestamp'
    ]) }} as order_review_sk,
    review_id,
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp
from {{ ref('raw_order_reviews') }}

{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key([
    'review_id',
    'order_id',
    'review_score',
    'review_creation_date',
    'review_answer_timestamp'
]) }} not in (
    select order_review_sk from {{ this }}
)
{% endif %}