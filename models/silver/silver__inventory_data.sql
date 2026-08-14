{{ config(
    materialized='table'
) }}

WITH

/* ============================================================
   1. PRODUCT SNAPSHOT HISTORY
   ============================================================ */

product_snapshots AS (

    SELECT
        product_id,
        last_modified_date,
        raw_product_data,
        dbt_valid_from,
        dbt_valid_to,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM {{ ref('PRODUCTS_SNAPSHOT') }}

),

/* ============================================================
   2. EXTRACT PRODUCT STOCK DATA
============================================================ */

product_snapshot_data AS (

    SELECT

        product_id,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    raw_product_data:stock_quantity::VARCHAR
                ),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    raw_product_data:reorder_level::VARCHAR
                ),
                ''
            )
        ) AS reorder_level,

        /*
           Use the product's source last_modified_date
           as the business snapshot date.
        */

        CAST(
            last_modified_date AS DATE
        ) AS snapshot_date,

        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM product_snapshots

    WHERE product_id IS NOT NULL

),

/* ============================================================
   3. DATE RANGE
============================================================ */

date_bounds AS (

    SELECT
        MIN(snapshot_date) AS min_date,
        MAX(snapshot_date) AS max_date

    FROM product_snapshot_data

),

calendar AS (

    SELECT
        DATEADD(
            DAY,
            SEQ4(),
            min_date
        ) AS calendar_date

    FROM date_bounds,

    TABLE(
        GENERATOR(
            ROWCOUNT => 10000
        )
    )

    WHERE DATEADD(
        DAY,
        SEQ4(),
        min_date
    ) <= max_date

),

/* ============================================================
   4. PRODUCT + DATE GRID
============================================================ */

product_dates AS (

    SELECT

        p.product_id,
        c.calendar_date

    FROM (
        SELECT DISTINCT
            product_id
        FROM product_snapshot_data
    ) p

    CROSS JOIN calendar c

),

/* ============================================================
   5. CARRY FORWARD LATEST PRODUCT SNAPSHOT
============================================================ */

daily_snapshot AS (

    SELECT

        pd.product_id,
        pd.calendar_date AS stock_date,

        ps.stock_quantity AS ending_stock,
        ps.reorder_level,

        ps.snapshot_date,

        DATEDIFF(
            DAY,
            ps.snapshot_date,
            pd.calendar_date
        ) AS days_since_snapshot,

        ps.SOURCE_FILE,
        ps.LOADED_AT,
        ps.BATCH_ID

    FROM product_dates pd

    LEFT JOIN product_snapshot_data ps

        ON pd.product_id = ps.product_id

       AND ps.snapshot_date <= pd.calendar_date

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            pd.product_id,
            pd.calendar_date

        ORDER BY
            ps.snapshot_date DESC,
            ps.LOADED_AT DESC,
            ps.SOURCE_FILE DESC

    ) = 1

),

/* ============================================================
   6. BRONZE ORDERS
============================================================ */

order_source AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID

    FROM {{ ref('stg_bronze__orders_data') }}

),

/* ============================================================
   7. FLATTEN ORDERS
============================================================ */

flattened_orders AS (

    SELECT

        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        order_data.value AS order_data

    FROM order_source s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:orders_data
    ) AS order_data

),

/* ============================================================
   8. FLATTEN ORDER ITEMS
============================================================ */

flattened_order_items AS (

    SELECT

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    o.order_data:order_date::VARCHAR
                ),
                ''
            )
        ) AS order_date,

        LOWER(
            TRIM(
                o.order_data:status::VARCHAR
            )
        ) AS order_status,

        item.value AS item_data

    FROM flattened_orders o,

    LATERAL FLATTEN(
        INPUT => o.order_data:order_items
    ) AS item

),

/* ============================================================
   9. SOLD QUANTITY
   ONLY COMPLETED ORDERS
============================================================ */

sold_quantity AS (

    SELECT

        NULLIF(
            TRIM(
                item_data:product_id::VARCHAR
            ),
            ''
        ) AS product_id,

        order_date AS stock_date,

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

    FROM flattened_order_items

    WHERE order_status = 'completed'

    GROUP BY
        product_id,
        stock_date

),

/* ============================================================
   10. COMBINE STOCK + SALES
============================================================ */

combined AS (

    SELECT

        d.product_id,
        d.stock_date,

        d.ending_stock,
        d.reorder_level,

        d.snapshot_date,
        d.days_since_snapshot,

        COALESCE(
            s.sold_quantity,
            0
        ) AS sold_quantity,

        d.SOURCE_FILE,
        d.LOADED_AT,
        d.BATCH_ID

    FROM daily_snapshot d

    LEFT JOIN sold_quantity s

        ON d.product_id = s.product_id

       AND d.stock_date = s.stock_date

),

/* ============================================================
   11. BEGINNING STOCK
============================================================ */

with_beginning_stock AS (

    SELECT

        c.*,

        LAG(
            c.ending_stock
        ) OVER (
            PARTITION BY c.product_id
            ORDER BY c.stock_date
        ) AS beginning_stock

    FROM combined c

),

/* ============================================================
   12. INFER PURCHASED QUANTITY
============================================================ */

with_purchases AS (

    SELECT

        b.*,

        CASE

            WHEN b.beginning_stock IS NOT NULL

            THEN
                b.ending_stock
                - b.beginning_stock
                + b.sold_quantity

            ELSE NULL

        END AS purchased_quantity

    FROM with_beginning_stock b

),

/* ============================================================
   13. VALIDATION / FLAGS
============================================================ */

validated AS (

    SELECT

        p.*,

        CASE
            WHEN p.ending_stock < p.reorder_level
                THEN TRUE
            ELSE FALSE
        END AS low_stock_flag,

        CASE
            WHEN p.days_since_snapshot > 1
                THEN TRUE
            ELSE FALSE
        END AS snapshot_stale_flag,

        CASE

            WHEN p.ending_stock < 0
              OR p.beginning_stock < 0
              OR p.purchased_quantity < 0

            THEN TRUE

            ELSE FALSE

        END AS negative_balance_flag

    FROM with_purchases p

)

/* ============================================================
   FINAL SILVER INVENTORY TABLE
============================================================ */

SELECT

    product_id,
    stock_date,

    beginning_stock,
    ending_stock,

    sold_quantity,
    purchased_quantity,

    reorder_level,

    low_stock_flag,

    snapshot_date,
    days_since_snapshot,
    snapshot_stale_flag,

    negative_balance_flag,

    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM validated