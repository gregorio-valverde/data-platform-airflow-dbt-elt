{{ config(
    materialized='view',
    tags=['stg', 'hr', 'consulting_firms']
) }}

with source as (

    select *
    from {{ source('hr', 'src_hr_consulting_firms') }}

),

renamed_casted as (

    select
        nullif(trim(consulting_firm_tax_id), '') as consulting_firm_tax_id,
        nullif(initcap(trim(consulting_firm_legal_name)), '') as consulting_firm_legal_name,
        nullif(upper(trim(vendor_status)), '') as vendor_status,

        cast(extraction_date as date) as extraction_date,
        nullif(trim(source_system), '') as source_system

    from source

),

final as (

    select
        consulting_firm_tax_id,
        consulting_firm_legal_name,
        vendor_status,

        case
            when vendor_status in ('ACTIVE', 'ENABLED') then true
            when vendor_status in ('INACTIVE', 'DISABLED') then false
            else null
        end as is_active_vendor,

        extraction_date,
        source_system

    from renamed_casted

)

select *
from final
