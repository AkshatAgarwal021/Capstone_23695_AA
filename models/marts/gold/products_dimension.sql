{{ config(
    materialized='table'
) }}

WITH products AS (

    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        color,
        size,
        unit_price,
        cost_price,
        supplier_id

    FROM {{ ref('silver__products_data') }}

),

suppliers AS (

    SELECT
        supplier_id,
        supplier_name

    FROM {{ ref('silver__suppliers_data') }}

),

final AS (

    SELECT

        /*
           SURROGATE KEY
        */

        MD5(
            COALESCE(p.product_id, '')
        ) AS product_key,


        /*
           NATURAL KEY
        */

        p.product_id,


        /*
           PRODUCT ATTRIBUTES
        */

        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.color,
        p.size,


        /*
           FINANCIAL ATTRIBUTES
        */

        p.unit_price,
        p.cost_price,


        /*
           SUPPLIER INFORMATION

           Supplier information is resolved from
           the Silver Supplier table.
        */

        p.supplier_id,
        s.supplier_name

    FROM products p

    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id

)

SELECT *

FROM final