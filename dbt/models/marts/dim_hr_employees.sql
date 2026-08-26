{{ config(
    materialized='table',
    tags=['mart', 'hr', 'employees']
) }}

select
    employee_id as id_employee,
    department_code as id_department,
    full_name as name_employee,
    is_turnover,
    extraction_date,
    source_system
from {{ ref('stg_hr_employees') }}
