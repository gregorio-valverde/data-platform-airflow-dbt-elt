{{ config(
    materialized='view',
    tags=['stg', 'hr', 'employees']
) }}

with source as (

    select *
    from {{ source('hr', 'src_hr_employees') }}

),

renamed_casted as (

    select
        cast(extraction_date as date) as extraction_date,
        nullif(trim(source_system), '') as source_system,
        nullif(trim(source_file), '') as source_file,
        cast(source_row_number as integer) as source_row_number,

        cast(employee_id as integer) as employee_id,

        nullif(initcap(trim(first_name)), '') as first_name,
        nullif(initcap(trim(last_name)), '') as last_name,

        nullif(upper(trim(department_code)), '') as department_code,
        nullif(initcap(trim(department_name)), '') as department_name,

        nullif(initcap(trim(employment_type)), '') as employment_type,

        cast(salary as numeric(14, 2)) as salary,
        cast(hourly_rate as numeric(14, 2)) as hourly_rate,
        cast(overtime_hourly_rate as numeric(14, 2)) as overtime_hourly_rate,

        nullif(trim(consulting_firm_tax_id), '') as consulting_firm_tax_id,
        nullif(initcap(trim(consulting_firm_legal_name)), '') as consulting_firm_legal_name,

        nullif(upper(trim(performance_rating_2024)), '') as performance_rating_2024,
        nullif(upper(trim(performance_rating_2023)), '') as performance_rating_2023,
        nullif(upper(trim(performance_rating_2022)), '') as performance_rating_2022,
        nullif(upper(trim(performance_rating_2021)), '') as performance_rating_2021,
        nullif(upper(trim(performance_rating_2020)), '') as performance_rating_2020,

        cast(turnover_flag as integer) as turnover_flag

    from source

),

final as (

    select
        extraction_date,
        source_system,
        source_file,
        source_row_number,
        employee_id,

        first_name,
        last_name,
        concat_ws(' ', first_name, last_name) as full_name,

        department_code,
        department_name,
        employment_type,

        salary,
        hourly_rate,
        overtime_hourly_rate,

        consulting_firm_tax_id,
        consulting_firm_legal_name,

        performance_rating_2024,
        performance_rating_2023,
        performance_rating_2022,
        performance_rating_2021,
        performance_rating_2020,

        turnover_flag,
        case
            when turnover_flag = 1 then true
            when turnover_flag = 0 then false
            else null
        end as is_turnover

    from renamed_casted

)

select *
from final
