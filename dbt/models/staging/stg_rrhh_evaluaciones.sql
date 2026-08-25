{{ config(
    materialized='view',
    tags=['stg', 'rrhh', 'evaluaciones']
) }}

with source as (

    select *
    from {{ source('rrhh', 'src_rrhh_evaluaciones') }}

),

renamed_casted as (

    select
        nullif(trim(sistema_origen), '') as sistema_origen,
        cast(fecha_extraccion as date) as fecha_extraccion,

        cast(nro_linea_archivo as integer) as nro_linea_archivo,
        cast(legajo as integer) as legajo,

        nullif(initcap(trim(nombre)), '') as nombre,
        nullif(initcap(trim(apellido)), '') as apellido,

        nullif(upper(trim(departamento_codigo)), '') as departamento_codigo,

        cast(anio_evaluacion as integer) as anio_evaluacion,
        nullif(upper(trim(resultado_evaluacion)), '') as resultado_evaluacion

    from source

),

final as (

    select
        sistema_origen,
        fecha_extraccion,
        nro_linea_archivo,

        legajo,
        nombre,
        apellido,
        concat_ws(' ', nombre, apellido) as nombre_completo,

        departamento_codigo,
        anio_evaluacion,
        resultado_evaluacion,

        case
            when anio_evaluacion is not null
                then make_date(anio_evaluacion, 1, 1)
            else null
        end as fecha_inicio_anio_evaluacion,

        case
            when resultado_evaluacion is not null then true
            else false
        end as tiene_evaluacion

    from renamed_casted

)

select *
from final