select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'customer_zip_code_prefix',
        'customer_city',
        'customer_state'
    ]) }} as customer_address_sk,
    customer_id as customer_address_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from {{ ref('raw_customers') }}