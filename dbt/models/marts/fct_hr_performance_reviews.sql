{{ config(
    materialized='table',
    tags=['mart', 'hr', 'performance_reviews']
) }}

select
    employee_id as id_employee,
    review_year,
    performance_rating,
    has_performance_rating,
    extraction_date,
    source_system
from {{ ref('stg_hr_performance_reviews') }}
