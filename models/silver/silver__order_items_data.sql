{{ config(
    materialized='table'
) }}

WITH source_data AS (

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
*/

orders_flattened AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        order_data.value AS order_data

    FROM source_data,

    LATERAL FLATTEN(
        INPUT => RAW_DATA:orders_data
    ) AS order_data

),

/*
   2. FLATTEN ORDER ITEMS

   One row per order item.
*/

order_items_flattened AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,


        /*
           ORDER HEADER ATTRIBUTES
        */

        NULLIF(
            TRIM(
                order_data:order_id::VARCHAR
            ),
            ''
        ) AS order_id,


        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    order_data:order_date::VARCHAR
                ),
                ''
            )
        ) AS order_date,


        LOWER(
            TRIM(
                order_data:order_status::VARCHAR
            )
        ) AS order_status,


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


        /*
           ITEM POSITION

           Used to uniquely identify an item within an order.
        */

        item.index::NUMBER AS item_number,


        /*
           PRODUCT
        */

        NULLIF(
            TRIM(
                item.value:product_id::VARCHAR
            ),
            ''
        ) AS product_id,


        /*
           QUANTITY
        */

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    item.value:quantity::VARCHAR
                ),
                ''
            )
        ) AS quantity,


        /*
           UNIT PRICE

           Parse currency values.
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        item.value:unit_price::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS unit_price,


        /*
           COST PRICE

           Parse currency values.
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        item.value:cost_price::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS cost_price,


        /*
           DISCOUNT PERCENTAGE

           Source field is a percentage.

           Example:
           15 -> 15%
        */

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    item.value:discount_amount::VARCHAR
                ),
                ''
            ),
            10,
            4
        ) AS discount_percentage,


        /*
           DISCOUNT RATE

           Convert percentage into fraction.

           Example:
           15 -> 0.15
        */

        (
            COALESCE(
                TRY_TO_DECIMAL(
                    NULLIF(
                        TRIM(
                            item.value:discount_amount::VARCHAR
                        ),
                        ''
                    ),
                    10,
                    4
                ),
                0
            ) / 100
        ) AS discount_rate

    FROM orders_flattened,

    LATERAL FLATTEN(
        INPUT => order_data:order_items
    ) AS item

)

/*
   FINAL SILVER ORDER ITEMS TABLE
*/

SELECT

    /*
       SURROGATE ORDER ITEM KEY

       Order ID + item number uniquely identify
       an item within an order.
    */

    {{ dbt_utils.generate_surrogate_key([
        'order_id',
        'item_number'
    ]) }} AS order_item_key,


    /*
       SOURCE METADATA
    */

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID,


    /*
       ORDER ATTRIBUTES
    */

    order_id,
    item_number,

    order_date,
    order_status,

    customer_id,
    store_id,


    /*
       PRODUCT ATTRIBUTES
    */

    product_id,


    /*
       ITEM MEASURES
    */

    quantity,
    unit_price,
    cost_price,

    discount_percentage,
    discount_rate

FROM order_items_flattened