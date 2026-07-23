select
    {{ dbt_utils.generate_surrogate_key([
        'p.product_id',
        't.product_category_name_english',
        'p.product_name_lenght',
        'p.product_description_lenght',
        'p.product_photos_qty',
        'p.product_weight_g',
        'p.product_length_cm',
        'p.product_height_cm',
        'p.product_width_cm'
    ]) }} as product_sk,
    p.product_id,
    case
        when p.product_category_name = 'portateis_cozinha_e_preparadores_de_alimentos' 
            then 'portable kitchen appliances and food preparers'
        when p.product_category_name = 'pc_gamer' 
            then 'pc_gamer'
        else t.product_category_name_english
    end as product_category_name,
    p.product_name_lenght as product_name_length,
    p.product_description_lenght as product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from {{ ref('raw_products') }} p
left join {{ ref('raw_product_category_name_translation') }} t
    on p.product_category_name = t.product_category_name
where p.product_category_name is not null