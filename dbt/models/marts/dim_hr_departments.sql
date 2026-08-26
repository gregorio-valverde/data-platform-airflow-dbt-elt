{{ config(
    materialized='table',
    tags=['mart', 'hr', 'departments']
) }}

select
    department_code as id_department,
    department_name as name_department,
    is_active_department as active,
    extraction_date,
    source_system
from {{ ref('stg_hr_departments') }}
