{{ config(
    materialized='view',
    tags=['stg', 'hr', 'payroll']
) }}

with source as (

    select *
    from {{ source('hr', 'src_hr_payroll') }}

),

renamed_casted as (

    select
        nullif(trim(source_system), '') as source_system,
        cast(extraction_date as date) as extraction_date,

        nullif(trim(payroll_receipt_id), '') as payroll_receipt_id,
        nullif(trim(payroll_period), '') as payroll_period,

        cast(employee_id as integer) as employee_id,

        nullif(initcap(trim(first_name)), '') as first_name,
        nullif(initcap(trim(last_name)), '') as last_name,

        nullif(upper(trim(department_code)), '') as department_code,
        nullif(initcap(trim(employment_type)), '') as employment_type,

        nullif(trim(consulting_firm_tax_id), '') as consulting_firm_tax_id,
        nullif(upper(trim(payroll_status)), '') as payroll_status,

        cast(paid_days as integer) as paid_days,

        cast(monthly_salary as numeric(14, 2)) as monthly_salary,
        cast(regular_hours as numeric(10, 2)) as regular_hours,
        cast(regular_hours_amount as numeric(14, 2)) as regular_hours_amount,
        cast(overtime_hours as numeric(10, 2)) as overtime_hours,
        cast(overtime_amount as numeric(14, 2)) as overtime_amount,
        cast(productivity_bonus as numeric(14, 2)) as productivity_bonus,
        cast(absence_deduction as numeric(14, 2)) as absence_deduction,
        cast(gross_pay as numeric(14, 2)) as gross_pay,
        cast(estimated_employer_cost as numeric(14, 2)) as estimated_employer_cost

    from source

),

final as (

    select
        source_system,
        extraction_date,

        payroll_receipt_id,
        payroll_period,

        case
            when payroll_period ~ '^[0-9]{4}-[0-9]{2}$'
                then to_date(payroll_period || '-01', 'YYYY-MM-DD')
            when payroll_period ~ '^[0-9]{6}$'
                then to_date(payroll_period || '01', 'YYYYMMDD')
            else null
        end as payroll_period_date,

        employee_id,

        first_name,
        last_name,
        concat_ws(' ', first_name, last_name) as full_name,

        department_code,
        employment_type,
        consulting_firm_tax_id,
        payroll_status,

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

        coalesce(regular_hours_amount, 0)
            + coalesce(overtime_amount, 0)
            + coalesce(productivity_bonus, 0)
            - coalesce(absence_deduction, 0) as calculated_gross_pay,

        estimated_employer_cost - gross_pay as employer_cost_variance

    from renamed_casted

)

select *
from final
