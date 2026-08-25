{{ config(
    materialized='view',
    tags=['stg', 'rrhh', 'nominas']
) }}

with source as (

    select *
    from {{ source('rrhh', 'src_rrhh_nominas') }}

),

renamed_casted as (

    select
        nullif(trim(sistema_origen), '') as sistema_origen,
        cast(fecha_extraccion as date) as fecha_extraccion,

        nullif(trim(recibo_nomina_id), '') as recibo_nomina_id,
        nullif(trim(periodo), '') as periodo,

        cast(legajo as integer) as legajo,

        nullif(initcap(trim(nombre)), '') as nombre,
        nullif(initcap(trim(apellido)), '') as apellido,

        nullif(upper(trim(departamento_codigo)), '') as departamento_codigo,
        nullif(initcap(trim(tipo_empleado)), '') as tipo_empleado,

        nullif(trim(consultora_cuit), '') as consultora_cuit,
        nullif(initcap(trim(estado_liquidacion)), '') as estado_liquidacion,

        cast(dias_liquidados as integer) as dias_liquidados,

        cast(salario_mensual as numeric(14, 2)) as salario_mensual,
        cast(horas_base as numeric(10, 2)) as horas_base,
        cast(importe_horas_base as numeric(14, 2)) as importe_horas_base,
        cast(horas_extra as numeric(10, 2)) as horas_extra,
        cast(importe_horas_extra as numeric(14, 2)) as importe_horas_extra,
        cast(bono_productividad as numeric(14, 2)) as bono_productividad,
        cast(descuento_ausencias as numeric(14, 2)) as descuento_ausencias,
        cast(total_bruto as numeric(14, 2)) as total_bruto,
        cast(coste_empresa_estimado as numeric(14, 2)) as coste_empresa_estimado

    from source

),

final as (

    select
        sistema_origen,
        fecha_extraccion,

        recibo_nomina_id,
        periodo,

        case
            when periodo ~ '^[0-9]{4}-[0-9]{2}$'
                then to_date(periodo || '-01', 'YYYY-MM-DD')
            when periodo ~ '^[0-9]{6}$'
                then to_date(periodo || '01', 'YYYYMMDD')
            else null
        end as fecha_periodo,

        legajo,

        nombre,
        apellido,
        concat_ws(' ', nombre, apellido) as nombre_completo,

        departamento_codigo,
        tipo_empleado,
        consultora_cuit,
        estado_liquidacion,

        dias_liquidados,

        salario_mensual,
        horas_base,
        importe_horas_base,
        horas_extra,
        importe_horas_extra,
        bono_productividad,
        descuento_ausencias,
        total_bruto,
        coste_empresa_estimado,

        coalesce(importe_horas_base, 0)
            + coalesce(importe_horas_extra, 0)
            + coalesce(bono_productividad, 0)
            - coalesce(descuento_ausencias, 0) as total_bruto_calculado,

        coste_empresa_estimado - total_bruto as diferencia_coste_empresa_bruto

    from renamed_casted

)

select *
from final