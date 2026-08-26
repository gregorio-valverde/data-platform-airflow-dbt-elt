{{ config(
    materialized='view',
    tags=['stg', 'hr', 'performance_reviews']
) }}

with source as (

    select *
    from {{ source('hr', 'src_hr_performance_reviews') }}

),

renamed_casted as (

    select
        nullif(trim(source_system), '') as source_system,
        cast(extraction_date as date) as extraction_date,

        cast(source_row_number as integer) as source_row_number,
        cast(employee_id as integer) as employee_id,

        nullif(initcap(trim(first_name)), '') as first_name,
        nullif(initcap(trim(last_name)), '') as last_name,

        nullif(upper(trim(department_code)), '') as department_code,

        cast(review_year as integer) as review_year,
        nullif(upper(trim(performance_rating)), '') as performance_rating

    from source

),

final as (

    select
        source_system,
        extraction_date,
        source_row_number,

        employee_id,
        first_name,
        last_name,
        concat_ws(' ', first_name, last_name) as full_name,

        department_code,
        review_year,
        performance_rating,

        case
            when review_year is not null
                then make_date(review_year, 1, 1)
            else null
        end as review_year_start_date,

        case
            when performance_rating is not null then true
            else false
        end as has_performance_rating

    from renamed_casted

)

select *
from final
