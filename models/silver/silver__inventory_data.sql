{{ config(
    materialized='table'
) }}

WITH product_history AS (

    SELECT
        product_history_key,
        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        supplier_id,
        cost_price

    FROM {{ ref('silver__products_history_data') }}

),

/*
   1. PRODUCT / STORE RELATIONSHIP

   Product History has no store_id because
   Product JSON does not contain one.

   Store association comes from Order Items.
*/

product_store AS (

    SELECT DISTINCT

        product_id,
        store_id

    FROM {{ ref('silver__order_items_data') }}

    WHERE product_id IS NOT NULL
      AND store_id IS NOT NULL

),

/*
   2. PRODUCT HISTORY + PRODUCT/STORE RELATIONSHIP

   This creates the required:
   product + store + snapshot date
   inventory grain.

   IMPORTANT:
   The stock snapshot is associated with each
   observed product/store relationship.

*/

inventory_snapshots AS (

    SELECT

        ph.product_id,
        ps.store_id,

        ph.source_snapshot_date AS inventory_date,

        ph.stock_quantity AS ending_stock,

        ph.reorder_level,
        ph.supplier_id,
        ph.cost_price

    FROM product_history ph

    INNER JOIN product_store ps
        ON ph.product_id = ps.product_id

),

/*
   3. BEGINNING INVENTORY

   Previous snapshot's ending stock for the
   same product/store combination.
*/

with_beginning_inventory AS (

    SELECT

        product_id,
        store_id,
        inventory_date,

        LAG(ending_stock) OVER (

            PARTITION BY
                product_id,
                store_id

            ORDER BY
                inventory_date

        ) AS beginning_stock,

        ending_stock,

        reorder_level,
        supplier_id,
        cost_price

    FROM inventory_snapshots

),

/*
   4. COMPLETED ORDER ITEM SALES

   Store ID comes directly from Order Items,
   which inherited it from Orders.
*/

completed_sales AS (

    SELECT

        product_id,
        store_id,
        order_date AS inventory_date,

        SUM(quantity) AS sold_quantity

    FROM {{ ref('silver__order_items_data') }}

    WHERE LOWER(order_status) IN (
        'completed',
        'delivered'
    )

      AND product_id IS NOT NULL
      AND store_id IS NOT NULL
      AND order_date IS NOT NULL

    GROUP BY

        product_id,
        store_id,
        order_date

),

/*
   5. COMBINE STOCK + SALES

   LEFT JOIN is intentional because a product/store
   may have inventory on a date but no completed sale.
*/

combined AS (

    SELECT

        b.product_id,
        b.store_id,
        b.inventory_date,

        b.beginning_stock,

        COALESCE(
            s.sold_quantity,
            0
        ) AS sold_quantity,

        b.ending_stock,

        b.reorder_level,
        b.supplier_id,
        b.cost_price

    FROM with_beginning_inventory b

    LEFT JOIN completed_sales s

        ON b.product_id = s.product_id

       AND b.store_id = s.store_id

       AND b.inventory_date = s.inventory_date

),

/*
   6. INVENTORY BUSINESS CALCULATIONS
*/

calculated AS (

    SELECT

        product_id,
        store_id,
        inventory_date,

        beginning_stock,

        sold_quantity,

        ending_stock,


        /*
           PURCHASED QUANTITY

           Ending
           - Beginning
           + Sold
        */

        (
            COALESCE(ending_stock, 0)
            - COALESCE(beginning_stock, 0)
            + COALESCE(sold_quantity, 0)
        ) AS purchased_quantity,


        /*
           INVENTORY VALUE
        */

        CASE

            WHEN ending_stock IS NOT NULL
             AND cost_price IS NOT NULL

            THEN ending_stock * cost_price

            ELSE NULL

        END AS inventory_value,


        /*
           AVERAGE INVENTORY
        */

        (
            COALESCE(beginning_stock, 0)
            + COALESCE(ending_stock, 0)
        ) / 2.0 AS average_inventory,


        reorder_level,
        supplier_id,
        cost_price

    FROM combined

),

/*
   7. FINAL DERIVED METRICS
*/

final AS (

    SELECT

        /*
           FACT GRAIN:
           Product + Store + Date
        */

        {{ dbt_utils.generate_surrogate_key([
            'product_id',
            'store_id',
            'inventory_date'
        ]) }} AS inventory_key,

        product_id,
        store_id,
        inventory_date,

        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,

        inventory_value,


        /*
           STOCK TURNOVER RATIO
        */

        CASE

            WHEN average_inventory > 0

            THEN sold_quantity / average_inventory

            ELSE NULL

        END AS stock_turnover_ratio,


        /*
           SUPPLIER CONTRIBUTION PERCENTAGE

           Calculated later from purchased quantity
           at the supplier/product/store/date grain.
        */

        CASE

            WHEN purchased_quantity > 0

            THEN 100.0

            ELSE 0.0

        END AS supplier_contribution_percentage,


        reorder_level,
        supplier_id,


        /*
           SNAPSHOT GAP

           Compare the current snapshot to the
           previous snapshot for this product/store.
        */

        CASE

            WHEN LAG(inventory_date) OVER (

                PARTITION BY
                    product_id,
                    store_id

                ORDER BY
                    inventory_date

            ) IS NULL

            THEN FALSE

            WHEN DATEDIFF(
                DAY,
                LAG(inventory_date) OVER (

                    PARTITION BY
                        product_id,
                        store_id

                    ORDER BY
                        inventory_date

                ),
                inventory_date
            ) > 1

            THEN TRUE

            ELSE FALSE

        END AS snapshot_gap_flag,


        CASE

            WHEN LAG(inventory_date) OVER (

                PARTITION BY
                    product_id,
                    store_id

                ORDER BY
                    inventory_date

            ) IS NULL

            THEN 0

            ELSE DATEDIFF(
                DAY,
                LAG(inventory_date) OVER (

                    PARTITION BY
                        product_id,
                        store_id

                    ORDER BY
                        inventory_date

                ),
                inventory_date
            )

        END AS snapshot_gap_days,


        /*
           LOW STOCK
        */

        CASE

            WHEN ending_stock IS NOT NULL
             AND reorder_level IS NOT NULL
             AND ending_stock < reorder_level

            THEN TRUE

            ELSE FALSE

        END AS low_stock_flag,


        /*
           NEGATIVE INFERRED PURCHASE
        */

        CASE

            WHEN purchased_quantity < 0

            THEN TRUE

            ELSE FALSE

        END AS negative_inferred_purchase_flag

    FROM calculated

)

SELECT *

FROM final