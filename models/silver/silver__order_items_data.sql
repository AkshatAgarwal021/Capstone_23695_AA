{{ config(
    materialized='table'
) }}

WITH order_items_source AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        RAW_DATA
    FROM {{ ref('stg_bronze__orders_data') }}

),

/*
   1. FLATTEN ORDERS

   We need the complete order object because
   store_id exists at the order header level.
*/

flattened_orders AS (

    SELECT

        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        order_data.value AS order_data

    FROM order_items_source s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:orders_data
    ) AS order_data

),

/*
   2. EXTRACT ORDER HEADER INFORMATION

   store_id comes from the parent Order.
*/

order_header AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(
                order_data:order_id::VARCHAR
            ),
            ''
        ) AS order_id,

        NULLIF(
            TRIM(
                order_data:customer_id::VARCHAR
            ),
            ''
        ) AS customer_id,

        NULLIF(
            TRIM(
                order_data:store_id::VARCHAR
            ),
            ''
        ) AS store_id,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    order_data:order_date::VARCHAR
                ),
                ''
            )
        ) AS order_date,

        NULLIF(
            TRIM(
                order_data:order_status::VARCHAR
            ),
            ''
        ) AS order_status,

        order_data:order_items AS order_items

    FROM flattened_orders

),

/*
   3. FLATTEN ORDER ITEMS

   FLATTEN.INDEX gives us the item's position
   within the order_items array.

   We convert it to a human-readable item_number.
*/

flattened_items AS (

    SELECT

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        o.order_id,
        o.customer_id,
        o.store_id,
        o.order_date,
        o.order_status,

        item.index + 1 AS item_number,

        item.value AS item_data

    FROM order_header o,

    LATERAL FLATTEN(
        INPUT => o.order_items
    ) AS item

),

/*
   4. CLEAN ORDER ITEMS
*/

cleaned AS (

    SELECT

        /*
           ORDER ITEM KEY

           Natural grain:
           order_id + item_number
        */

        {{ dbt_utils.generate_surrogate_key([
            'order_id',
            'item_number'
        ]) }} AS order_item_key,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        order_id,
        item_number,

        order_date,
        order_status,
        customer_id,

        /*
           STORE ID COMES FROM THE PARENT ORDER
        */

        store_id,


        /*
           PRODUCT ID COMES FROM THE ORDER ITEM
        */

        NULLIF(
            TRIM(
                item_data:product_id::VARCHAR
            ),
            ''
        ) AS product_id,


        /*
           QUANTITY
        */

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
        ) AS quantity,


        /*
           UNIT PRICE
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(
                            item_data:unit_price::VARCHAR
                        ),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS unit_price,


        /*
           COST PRICE
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(
                            item_data:cost_price::VARCHAR
                        ),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS cost_price,


        /*
           DISCOUNT

           Preserve existing project convention.
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(
                        item_data:discount_amount::VARCHAR
                    ),
                    ''
                ),
                18,
                6
            ),
            0
        ) AS discount_percentage

    FROM flattened_items

),

/*
   5. CONVERT DISCOUNT PERCENTAGE TO RATE
*/

derived AS (

    SELECT

        c.*,

        CASE

            WHEN c.discount_percentage IS NOT NULL

            THEN c.discount_percentage / 100

            ELSE 0

        END AS discount_rate

    FROM cleaned c

),

/*
   6. DEDUPLICATION

   One row per order + item number.

   This protects Inventory sold_quantity
   from duplicate historical versions of
   the same order item.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            order_id,
            item_number

        ORDER BY
            order_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/*
   FINAL SILVER ORDER ITEMS TABLE
*/

SELECT

    order_item_key,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID,

    order_id,
    item_number,

    order_date,
    order_status,
    customer_id,
    store_id,

    product_id,
    quantity,

    unit_price,
    cost_price,

    discount_percentage,
    discount_rate

FROM deduplicated