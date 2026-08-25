{{ config(
    materialized='view',
    tags=['stg', 'rrhh', 'consultoras']
) }}

with source as (

    select *
    from {{ source('rrhh', 'src_rrhh_consultoras') }}

),

renamed_casted as (

    select
        nullif(trim(consultora_cuit), '') as consultora_cuit,
        nullif(initcap(trim(consultora_razon_social)), '') as consultora_razon_social,
        nullif(initcap(trim(estado_proveedor)), '') as estado_proveedor,

        cast(fecha_extraccion as date) as fecha_extraccion,
        nullif(trim(sistema_origen), '') as sistema_origen

    from source

),

final as (

    select
        consultora_cuit,
        consultora_razon_social,
        estado_proveedor,

        case
            when lower(estado_proveedor) in ('activo', 'activa', 'alta', 'habilitado', 'habilitada') then true
            when lower(estado_proveedor) in ('inactivo', 'inactiva', 'baja', 'deshabilitado', 'deshabilitada') then false
            else null
        end as es_proveedor_activo,

        fecha_extraccion,
        sistema_origen

    from renamed_casted

)

select *
from final