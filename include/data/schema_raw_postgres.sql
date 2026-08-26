create schema if not exists raw;

create table if not exists raw.src_hr_employees (
    extraction_date date,
    source_system text,
    source_file text,
    source_row_number integer,
    employee_id integer,
    first_name text,
    last_name text,
    department_code text,
    department_name text,
    employment_type text,
    salary numeric(14,2),
    hourly_rate numeric(14,2),
    overtime_hourly_rate numeric(14,2),
    consulting_firm_tax_id text,
    consulting_firm_legal_name text,
    performance_rating_2024 text,
    performance_rating_2023 text,
    performance_rating_2022 text,
    performance_rating_2021 text,
    performance_rating_2020 text,
    turnover_flag integer
);

create table if not exists raw.src_hr_departments (
    department_code text,
    department_name text,
    is_active text,
    extraction_date date,
    source_system text
);

create table if not exists raw.src_hr_consulting_firms (
    consulting_firm_tax_id text,
    consulting_firm_legal_name text,
    vendor_status text,
    extraction_date date,
    source_system text
);

create table if not exists raw.src_hr_performance_reviews (
    source_system text,
    extraction_date date,
    source_row_number integer,
    employee_id integer,
    first_name text,
    last_name text,
    department_code text,
    review_year integer,
    performance_rating text
);

create table if not exists raw.src_hr_payroll (
    source_system text,
    extraction_date date,
    payroll_receipt_id text,
    payroll_period text,
    employee_id integer,
    first_name text,
    last_name text,
    department_code text,
    employment_type text,
    consulting_firm_tax_id text,
    payroll_status text,
    paid_days integer,
    monthly_salary numeric(14,2),
    regular_hours numeric(10,2),
    regular_hours_amount numeric(14,2),
    overtime_hours numeric(10,2),
    overtime_amount numeric(14,2),
    productivity_bonus numeric(14,2),
    absence_deduction numeric(14,2),
    gross_pay numeric(14,2),
    estimated_employer_cost numeric(14,2)
);
