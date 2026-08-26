{{ config(
    materialized='table',
    tags=['mart', 'hr', 'consulting_firms']
) }}

select
    consulting_firm_tax_id as id_consulting_firm_tax,
    consulting_firm_legal_name as name_consulting_firm,
    is_active_vendor as active,
    extraction_date,
    source_system
from {{ ref('stg_hr_consulting_firms') }}
