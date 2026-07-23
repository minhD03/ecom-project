select
    {{ dbt_utils.generate_surrogate_key([
        'seller_id',
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state'
    ]) }} as seller_sk,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from {{ ref('raw_sellers') }}