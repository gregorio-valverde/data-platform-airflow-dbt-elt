{{ config(
    materialized='view',
    tags=['stg', 'hr', 'departments']
) }}

with source as (

    select *
    from {{ source('hr', 'src_hr_departments') }}

),

renamed_casted as (

    select
        nullif(upper(trim(department_code)), '') as department_code,
        nullif(initcap(trim(department_name)), '') as department_name,
        nullif(lower(trim(is_active)), '') as is_active,

        cast(extraction_date as date) as extraction_date,
        nullif(trim(source_system), '') as source_system

    from source

),

final as (

    select
        department_code,
        department_name,

        is_active as raw_active_status,

        case
            when is_active in ('1', 'true', 't', 'yes', 'y', 'active') then true
            when is_active in ('0', 'false', 'f', 'no', 'n', 'inactive') then false
            else null
        end as is_active_department,

        extraction_date,
        source_system

    from renamed_casted

)

select *
from final
