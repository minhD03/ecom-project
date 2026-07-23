select
    {{ dbt_utils.generate_surrogate_key([
        'customer_unique_id',
        'customer_id'
    ]) }} as customer_sk,
    customer_unique_id as customer_id,
    customer_id as customer_address_id
from {{ ref('raw_customers') }}