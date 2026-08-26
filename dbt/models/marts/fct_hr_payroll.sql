{{ config(
    materialized='table',
    tags=['mart', 'hr', 'payroll']
) }}

select
    payroll_receipt_id as id_payroll,
    payroll_period_date as date_payroll,
    employee_id as id_employee,
    payroll_status as status,
    paid_days,
    monthly_salary,
    regular_hours,
    regular_hours_amount,
    overtime_hours,
    overtime_amount,
    productivity_bonus,
    absence_deduction,
    gross_pay,
    estimated_employer_cost,
    calculated_gross_pay,
    employer_cost_variance,
    extraction_date,
    source_system
from {{ ref('stg_hr_payroll') }}
