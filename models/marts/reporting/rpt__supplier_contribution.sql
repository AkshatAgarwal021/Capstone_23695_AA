{{ config(
    materialized='view',
    schema='gold'
) }}

WITH supplier_category AS (

    SELECT

        fi.supplier_key,
        dsp.supplier_id,
        dsp.supplier_name,

        fi.product_key,
        dp.product_id,
        dp.product_name,
        dp.category,

        SUM(
            fi.purchased_quantity
        ) AS supplier_purchased_quantity

    FROM {{ ref('Fact_Inventory') }} fi

    LEFT JOIN {{ ref('Dim_Products') }} dp

        ON fi.product_key =
           dp.product_key

    LEFT JOIN {{ ref('Dim_Suppliers') }} dsp

        ON fi.supplier_key =
           dsp.supplier_key

    GROUP BY

        fi.supplier_key,
        dsp.supplier_id,
        dsp.supplier_name,

        fi.product_key,
        dp.product_id,
        dp.product_name,
        dp.category

),

category_totals AS (

    SELECT

        category,

        SUM(
            supplier_purchased_quantity
        ) AS total_category_purchased_quantity

    FROM supplier_category

    GROUP BY

        category

)

SELECT

    sc.supplier_key,
    sc.supplier_id,
    sc.supplier_name,

    sc.category,

    sc.supplier_purchased_quantity,

    ct.total_category_purchased_quantity,

    CASE

        WHEN ct.total_category_purchased_quantity > 0

        THEN
            100.0
            * sc.supplier_purchased_quantity
            /
            NULLIF(
                ct.total_category_purchased_quantity,
                0
            )

        ELSE NULL

    END AS supplier_contribution_percentage

FROM supplier_category sc

LEFT JOIN category_totals ct

    ON sc.category =
       ct.category