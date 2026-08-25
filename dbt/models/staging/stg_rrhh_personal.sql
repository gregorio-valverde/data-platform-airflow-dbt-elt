{{ config(
    materialized='view',
    tags=['stg', 'rrhh', 'personal']
) }}

with source as (

    select *
    from {{ source('rrhh', 'src_rrhh_personal') }}

),

renamed_casted as (

    select
        cast(fecha_extraccion as date) as fecha_extraccion,
        nullif(trim(sistema_origen), '') as sistema_origen,
        nullif(trim(archivo_origen), '') as archivo_origen,
        cast(nro_linea_archivo as integer) as nro_linea_archivo,

        cast(legajo as integer) as legajo,

        nullif(initcap(trim(nombre)), '') as nombre,
        nullif(initcap(trim(apellido)), '') as apellido,

        nullif(upper(trim(departamento_codigo)), '') as departamento_codigo,
        nullif(initcap(trim(departamento_descripcion)), '') as departamento_descripcion,

        nullif(initcap(trim(tipo_empleado)), '') as tipo_empleado,

        cast(salario as numeric(14, 2)) as salario,
        cast(precio_hora as numeric(14, 2)) as precio_hora,
        cast(precio_hora_extra as numeric(14, 2)) as precio_hora_extra,

        nullif(trim(consultora_cuit), '') as consultora_cuit,
        nullif(initcap(trim(consultora_razon_social)), '') as consultora_razon_social,

        nullif(upper(trim(evaluacion_2024)), '') as evaluacion_2024,
        nullif(upper(trim(evaluacion_2023)), '') as evaluacion_2023,
        nullif(upper(trim(evaluacion_2022)), '') as evaluacion_2022,
        nullif(upper(trim(evaluacion_2021)), '') as evaluacion_2021,
        nullif(upper(trim(evaluacion_2020)), '') as evaluacion_2020,

        cast(rotacion as integer) as rotacion

    from source

),

final as (

    select
        fecha_extraccion,
        sistema_origen,
        archivo_origen,
        nro_linea_archivo,
        legajo,

        nombre,
        apellido,
        concat_ws(' ', nombre, apellido) as nombre_completo,

        departamento_codigo,
        departamento_descripcion,
        tipo_empleado,

        salario,
        precio_hora,
        precio_hora_extra,

        consultora_cuit,
        consultora_razon_social,

        evaluacion_2024,
        evaluacion_2023,
        evaluacion_2022,
        evaluacion_2021,
        evaluacion_2020,

        rotacion,
        case
            when rotacion = 1 then true
            when rotacion = 0 then false
            else null
        end as es_rotacion

    from renamed_casted

)

select *
from final