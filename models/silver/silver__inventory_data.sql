{{ config(
    materialized='table'
) }}

WITH

/* ============================================================
   1. PRODUCT SNAPSHOT HISTORY FROM BRONZE

   Each product source file represents a snapshot.

   Snapshot date is extracted from SOURCE_FILE.
   ============================================================ */

products_flattened AS (

    SELECT

        b.SOURCE_FILE,
        b.ROW_NUMBER,
        b.LOADED_AT,
        b.BATCH_ID,

        item.value:product_id::VARCHAR AS product_id,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    item.value:stock_quantity::VARCHAR
                ),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    item.value:reorder_level::VARCHAR
                ),
                ''
            )
        ) AS reorder_level,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                b.SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS snapshot_date

    FROM {{ ref('stg_bronze__products_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:products_data
    ) AS item

),

/* ============================================================
   2. DEDUPLICATE PRODUCT + SNAPSHOT DATE

   One product record per snapshot date.
   ============================================================ */

deduped_products AS (

    SELECT

        product_id,
        stock_quantity,
        reorder_level,
        snapshot_date,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID

    FROM products_flattened

    WHERE product_id IS NOT NULL
      AND snapshot_date IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            product_id,
            snapshot_date

        ORDER BY
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

),

/* ============================================================
   3. STOCK HISTORY

   Beginning stock = previous snapshot's ending stock
   Ending stock    = current snapshot stock
   ============================================================ */

stock_history AS (

    SELECT

        product_id,

        snapshot_date,

        reorder_level,

        LAG(
            stock_quantity
        ) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS beginning_stock,

        stock_quantity AS ending_stock,

        DATEDIFF(
            DAY,

            LAG(
                snapshot_date
            ) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            ),

            snapshot_date

        ) AS days_since_last_snapshot

    FROM deduped_products

),

/* ============================================================
   4. RAW ORDERS FROM BRONZE
   ============================================================ */

orders_source AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID

    FROM {{ ref('stg_bronze__orders_data') }}

),

/* ============================================================
   5. FLATTEN ORDERS

   Extract the natural order_id first because we need to
   deduplicate orders before calculating sold quantity.
   ============================================================ */

orders_flattened AS (

    SELECT

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        order_data.value:order_id::VARCHAR AS order_id,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    order_data.value:order_date::VARCHAR
                ),
                ''
            )
        ) AS order_datetime,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    order_data.value:order_date::VARCHAR
                ),
                ''
            )
        ) AS order_date,

        UPPER(
            TRIM(
                order_data.value:order_status::VARCHAR
            )
        ) AS order_status,

        order_data.value:order_items AS order_items

    FROM orders_source AS o,

    LATERAL FLATTEN(
        INPUT => o.RAW_DATA:orders_data
    ) AS order_data

),

/* ============================================================
   6. DEDUPLICATE ORDERS

   This is the important difference from our previous version.

   If the same order appears in multiple Bronze source files,
   only keep the latest copy.

   This makes Bronze behave like the "current orders" view
   used by your friend's solution.
   ============================================================ */

orders_current AS (

    SELECT

        order_id,
        order_date,
        order_status,
        order_items

    FROM orders_flattened

    WHERE order_id IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY order_id

        ORDER BY
            order_datetime DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

),

/* ============================================================
   7. FLATTEN ORDER ITEMS

   ============================================================ */

order_items_flattened AS (

    SELECT

        o.order_id,
        o.order_date,
        o.order_status,

        item.value AS item_data

    FROM orders_current AS o,

    LATERAL FLATTEN(
        INPUT => o.order_items
    ) AS item

),

/* ============================================================
   8. SOLD QUANTITY

   ONLY COMPLETED ORDERS.

   One product per day.
   ============================================================ */

sold_quantities AS (

    SELECT

        NULLIF(
            TRIM(
                item_data:product_id::VARCHAR
            ),
            ''
        ) AS product_id,

        order_date AS sold_date,

        SUM(
            COALESCE(
                TRY_TO_NUMBER(
                    NULLIF(
                        TRIM(
                            item_data:quantity::VARCHAR
                        ),
                        ''
                    )
                ),
                0
            )
        ) AS sold_quantity

    FROM order_items_flattened

    WHERE order_status = 'COMPLETED'

    GROUP BY

        product_id,
        sold_date

),

/* ============================================================
   9. JOIN STOCK HISTORY TO SOLD QUANTITY
   ============================================================ */

joined AS (

    SELECT

        s.product_id,

        s.snapshot_date,

        s.beginning_stock,

        s.ending_stock,

        s.reorder_level,

        s.days_since_last_snapshot,

        COALESCE(
            sq.sold_quantity,
            0
        ) AS sold_quantity

    FROM stock_history AS s

    LEFT JOIN sold_quantities AS sq

        ON s.product_id = sq.product_id

       AND s.snapshot_date = sq.sold_date

),

/* ============================================================
   10. CALCULATE PURCHASED QUANTITY
   ============================================================ */

calculated AS (

    SELECT

        product_id,

        snapshot_date,

        beginning_stock,

        ending_stock,

        sold_quantity,

        CASE

            WHEN beginning_stock IS NULL
                THEN NULL

            ELSE
                ending_stock
                - beginning_stock
                + sold_quantity

        END AS purchased_quantity,

        reorder_level,

        days_since_last_snapshot

    FROM joined

),

/* ============================================================
   11. VALIDATION + BUSINESS FLAGS
   ============================================================ */

validated AS (

    SELECT

        product_id,

        snapshot_date,

        beginning_stock,

        ending_stock,

        sold_quantity,

        purchased_quantity,

        reorder_level,

        /* Low stock */

        CASE

            WHEN ending_stock < reorder_level
                THEN TRUE

            ELSE FALSE

        END AS low_stock_flag,

        /*

           Snapshot gap handling.

           First snapshot has no prior inventory position.

           A gap > 1 day means the source snapshot is stale.
        */

        CASE

            WHEN beginning_stock IS NULL
                THEN TRUE

            WHEN days_since_last_snapshot > 1
                THEN TRUE

            ELSE FALSE

        END AS stale_snapshot_flag,

        days_since_last_snapshot,

        /* Negative inventory / inferred balance */

        CASE

            WHEN beginning_stock < 0
                THEN TRUE

            WHEN ending_stock < 0
                THEN TRUE

            WHEN purchased_quantity < 0
                THEN TRUE

            ELSE FALSE

        END AS negative_balance_flag

    FROM calculated

)

/* ============================================================
   FINAL SILVER INVENTORY TABLE
   ============================================================ */

SELECT

    product_id,

    snapshot_date,

    beginning_stock,
    ending_stock,

    sold_quantity,
    purchased_quantity,

    reorder_level,

    low_stock_flag,

    stale_snapshot_flag,
    days_since_last_snapshot,

    negative_balance_flag

FROM validated