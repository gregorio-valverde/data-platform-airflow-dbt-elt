{{ config(
    materialized='table',
    tags=['mart', 'hr', 'employees']
) }}

with source as (

    select *
    from {{ ref('stg_hr_employees') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by employee_id
            order by extraction_date desc, source_row_number desc
        ) as row_num
    from source

),

final as (

    select
        employee_id as id_employee,
        department_code as id_department,
        consulting_firm_tax_id as id_consulting_firm_tax,
        full_name as name_employee,
        employment_type,
        salary,
        hourly_rate,
        overtime_hourly_rate,
        is_turnover,
        extraction_date,
        source_system
    from deduplicated
    where row_num = 1

)

select *
from final