{{ config(
    materialized='table'
) }}

WITH inventory AS (

    SELECT

        inventory_key,
        product_id,
        store_id,
        inventory_date,

        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,

        inventory_value,
        stock_turnover_ratio,
        supplier_contribution_percentage,

        supplier_id

    FROM {{ ref('silver__inventory_data') }}

),

products AS (

    SELECT

        product_key,
        product_id

    FROM {{ ref('Dim_Products') }}

),

stores AS (

    SELECT

        store_key,
        store_id

    FROM {{ ref('Dim_Stores') }}

),

suppliers AS (

    SELECT

        supplier_key,
        supplier_id

    FROM {{ ref('Dim_Suppliers') }}

),

dates AS (

    SELECT

        date_key,
        full_date

    FROM {{ ref('Dim_date') }}

),

final AS (

    SELECT

        i.inventory_key,

        p.product_key,
        d.date_key,
        s.store_key,
        sup.supplier_key,

        i.beginning_stock,
        i.purchased_quantity,
        i.sold_quantity,
        i.ending_stock,

        i.inventory_value,
        i.stock_turnover_ratio,
        i.supplier_contribution_percentage

    FROM inventory i

    LEFT JOIN products p

        ON i.product_id = p.product_id

    LEFT JOIN stores s

        ON i.store_id = s.store_id

    LEFT JOIN suppliers sup

        ON i.supplier_id = sup.supplier_id

    LEFT JOIN dates d

        ON i.inventory_date = d.full_date

)

SELECT *

FROM final