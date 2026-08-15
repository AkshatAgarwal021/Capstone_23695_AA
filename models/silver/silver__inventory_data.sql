{{ config(
    materialized='table'
) }}

WITH product_history AS (

    SELECT

        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        supplier_id,
        cost_price,

        /*
           Beginning stock = previous snapshot stock
        */

        LAG(
            stock_quantity
        ) OVER (
            PARTITION BY product_id
            ORDER BY source_snapshot_date
        ) AS beginning_stock,

        /*
           Previous snapshot date
        */

        LAG(
            source_snapshot_date
        ) OVER (
            PARTITION BY product_id
            ORDER BY source_snapshot_date
        ) AS previous_snapshot_date

    FROM {{ ref('silver__products_history_data') }}

),

completed_sales AS (

    SELECT

        product_id,

        CAST(
            order_date AS DATE
        ) AS order_date,

        SUM(
            quantity
        ) AS sold_quantity

    FROM {{ ref('silver__order_items_data') }}

    WHERE LOWER(
        TRIM(order_status)
    ) = 'completed'

    GROUP BY

        product_id,
        CAST(order_date AS DATE)

),

combined AS (

    SELECT

        p.product_id,

        p.source_snapshot_date AS inventory_date,

        p.beginning_stock,

        p.stock_quantity AS ending_stock,

        COALESCE(
            s.sold_quantity,
            0
        ) AS sold_quantity,

        p.reorder_level,

        p.supplier_id,

        p.cost_price,

        p.previous_snapshot_date

    FROM product_history p

    LEFT JOIN completed_sales s

        ON p.product_id = s.product_id

       AND p.source_snapshot_date = s.order_date

),

derived AS (

    SELECT

        *,

        /*
           INFERRED PURCHASE QUANTITY

           purchased =
               ending_stock
               - beginning_stock
               + sold_quantity
        */

        CASE

            WHEN beginning_stock IS NOT NULL

            THEN
                ending_stock
                - beginning_stock
                + sold_quantity

            ELSE NULL

        END AS purchased_quantity,


        /*
           SNAPSHOT GAP FLAG

           TRUE when more than one day exists
           between consecutive product snapshots.
        */

        CASE

            WHEN previous_snapshot_date IS NULL
                THEN NULL

            WHEN DATEDIFF(
                DAY,
                previous_snapshot_date,
                inventory_date
            ) > 1

                THEN TRUE

            ELSE FALSE

        END AS snapshot_gap_flag,


        /*
           NUMBER OF DAYS BETWEEN SNAPSHOTS
        */

        CASE

            WHEN previous_snapshot_date IS NULL
                THEN NULL

            ELSE DATEDIFF(
                DAY,
                previous_snapshot_date,
                inventory_date
            )

        END AS snapshot_gap_days

    FROM combined

),

final AS (

    SELECT

        *,

        /*
           NEGATIVE INFERRED PURCHASE FLAG

           Do not silently correct negative inferred
           purchases. Flag them.
        */

        CASE

            WHEN purchased_quantity < 0
                THEN TRUE

            ELSE FALSE

        END AS negative_inferred_purchase_flag,


        /*
           LOW STOCK FLAG
        */

        CASE

            WHEN ending_stock IS NULL
              OR reorder_level IS NULL

                THEN NULL

            WHEN ending_stock < reorder_level

                THEN TRUE

            ELSE FALSE

        END AS low_stock_flag,


        /*
           INVENTORY VALUE

           ending_stock * cost_price
        */

        CASE

            WHEN ending_stock IS NOT NULL
             AND cost_price IS NOT NULL

            THEN
                ending_stock * cost_price

            ELSE NULL

        END AS inventory_value,


        /*
           AVERAGE INVENTORY

           (beginning_stock + ending_stock) / 2
        */

        CASE

            WHEN beginning_stock IS NOT NULL
             AND ending_stock IS NOT NULL

            THEN (
                beginning_stock + ending_stock
            ) / 2

            ELSE NULL

        END AS average_inventory

    FROM derived

),

metrics AS (

    SELECT

        *,

        /*
           STOCK TURNOVER RATIO

           sold_quantity / average_inventory

           Guard against division by zero.
        */

        CASE

            WHEN average_inventory > 0

            THEN
                sold_quantity
                / average_inventory

            ELSE NULL

        END AS stock_turnover_ratio,


        /*
           SUPPLIER CONTRIBUTION PERCENTAGE

           No observed receiving-event data exists in
           the supplied source structure.

           Therefore this remains NULL rather than
           inventing a contribution percentage.
        */

        CAST(
            NULL AS NUMBER(18,2)
        ) AS supplier_contribution_percentage

    FROM final

)

/*
   FINAL SILVER INVENTORY TABLE
*/

SELECT

    /*
       SURROGATE INVENTORY KEY

       One inventory row = one product per inventory date.
    */

    {{ dbt_utils.generate_surrogate_key([
        'product_id',
        'inventory_date'
    ]) }} AS inventory_key,


    /*
       GRAIN
    */

    product_id,
    inventory_date,


    /*
       INVENTORY MOVEMENT
    */

    beginning_stock,
    purchased_quantity,
    sold_quantity,
    ending_stock,


    /*
       INVENTORY VALUE / PERFORMANCE
    */

    inventory_value,

    stock_turnover_ratio,
    supplier_contribution_percentage,


    /*
       PRODUCT / SUPPLIER ATTRIBUTES
    */

    reorder_level,
    supplier_id,


    /*
       DATA QUALITY / SNAPSHOT FLAGS
    */

    snapshot_gap_flag,
    snapshot_gap_days,

    low_stock_flag,
    negative_inferred_purchase_flag

FROM metrics