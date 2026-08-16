{{ config(
    materialized='view',
    schema='gold'
) }}

SELECT

    fi.date_key,
    dd.full_date,

    fi.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,

    fi.store_key,
    ds.store_id,
    ds.store_name,

    fi.supplier_key,
    dsp.supplier_id,
    dsp.supplier_name,

    fi.ending_stock,
    fi.inventory_value

FROM {{ ref('Fact_Inventory') }} fi

LEFT JOIN {{ ref('Dim_date') }} dd

    ON fi.date_key =
       dd.date_key

LEFT JOIN {{ ref('Dim_Products') }} dp

    ON fi.product_key =
       dp.product_key

LEFT JOIN {{ ref('Dim_Stores') }} ds

    ON fi.store_key =
       ds.store_key

LEFT JOIN {{ ref('Dim_Suppliers') }} dsp

    ON fi.supplier_key =
       dsp.supplier_key