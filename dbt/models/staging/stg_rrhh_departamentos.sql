{{ config(
    materialized='view',
    tags=['stg', 'rrhh', 'departamentos']
) }}

with source as (

    select *
    from {{ source('rrhh', 'src_rrhh_departamentos') }}

),

renamed_casted as (

    select
        nullif(upper(trim(departamento_codigo)), '') as departamento_codigo,
        nullif(initcap(trim(departamento_descripcion)), '') as departamento_descripcion,
        nullif(lower(trim(activo)), '') as activo,

        cast(fecha_extraccion as date) as fecha_extraccion,
        nullif(trim(sistema_origen), '') as sistema_origen

    from source

),

final as (

    select
        departamento_codigo,
        departamento_descripcion,

        activo as activo_raw,

        case
            when activo in ('1', 'true', 't', 'yes', 'y', 'si', 'sí', 'activo', 'activa') then true
            when activo in ('0', 'false', 'f', 'no', 'n', 'inactivo', 'inactiva') then false
            else null
        end as es_departamento_activo,

        fecha_extraccion,
        sistema_origen

    from renamed_casted

)

select *
from final