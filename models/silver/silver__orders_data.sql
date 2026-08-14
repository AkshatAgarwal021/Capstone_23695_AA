{{ config(
    materialized='table'
) }}

WITH source_data AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__orders_data') }}

),

/*
   1. FLATTEN THE ORDERS ARRAY
*/

flattened_orders AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        order_data.value AS order_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:orders_data
    ) order_data

),

/*
   2. EXTRACT ORDER HEADER FIELDS
*/

order_header AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(order_data:order_id::VARCHAR),
            ''
        ) AS order_id,

        NULLIF(
            TRIM(order_data:customer_id::VARCHAR),
            ''
        ) AS customer_id,

        NULLIF(
            TRIM(order_data:store_id::VARCHAR),
            ''
        ) AS store_id,

        NULLIF(
            TRIM(order_data:employee_id::VARCHAR),
            ''
        ) AS employee_id,

        /*
           ORDER DATE/TIME

           Keep timestamp so that order hour can be derived.
        */

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(order_data:order_date::VARCHAR),
                ''
            )
        ) AS order_datetime,

        TRY_TO_DATE(
            NULLIF(
                TRIM(order_data:order_date::VARCHAR),
                ''
            )
        ) AS order_date,

        TRY_TO_DATE(
            NULLIF(
                TRIM(order_data:shipping_date::VARCHAR),
                ''
            )
        ) AS shipping_date,

        TRY_TO_DATE(
            NULLIF(
                TRIM(order_data:delivery_date::VARCHAR),
                ''
            )
        ) AS delivery_date,

        TRY_TO_DATE(
            NULLIF(
                TRIM(order_data:estimated_delivery_date::VARCHAR),
                ''
            )
        ) AS estimated_delivery_date,

        /*
           ORDER-LEVEL DISCOUNT

           Source value is a PERCENTAGE.
           Example:
               10 = 10%
               25 = 25%

           Convert percentage to fractional rate:
               10 / 100 = 0.10
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(order_data:discount_amount::VARCHAR),
                    ''
                ),
                18,
                6
            ) / 100,
            0
        ) AS order_discount_amount,

        /*
           SHIPPING COST
           Parse currency strings such as $24,005.75
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(order_data:shipping_cost::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS shipping_cost,

        /*
           TAX AMOUNT
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(order_data:tax_amount::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS tax_amount,

        order_data:order_items AS order_items

    FROM flattened_orders

),

/*
   3. FLATTEN ORDER ITEMS
*/

flattened_items AS (

    SELECT

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        o.order_id,

        item.value AS item_data

    FROM order_header o,

    LATERAL FLATTEN(
        INPUT => o.order_items
    ) item

),

/*
   4. CLEAN ORDER ITEMS
*/

cleaned_items AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        order_id,

        NULLIF(
            TRIM(item_data:product_id::VARCHAR),
            ''
        ) AS product_id,

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(item_data:quantity::VARCHAR),
                    ''
                )
            ),
            0
        ) AS quantity,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(item_data:unit_price::VARCHAR),
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

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(item_data:cost_price::VARCHAR),
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
           ITEM DISCOUNT

           Source value is a PERCENTAGE.
           Convert to fractional rate.

           Example:
               15 = 15%
               15 / 100 = 0.15
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(item_data:discount_amount::VARCHAR),
                    ''
                ),
                18,
                6
            ) / 100,
            0
        ) AS item_discount_amount

    FROM flattened_items

),

/*
   5. AGGREGATE ORDER ITEMS TO ORDER GRAIN
*/

order_item_aggregates AS (

    SELECT

        order_id,

        COUNT(product_id) AS total_items,

        SUM(quantity) AS total_quantity,

        SUM(
            quantity * unit_price
        ) AS total_amount,

        SUM(
            quantity * cost_price
        ) AS total_cost,

        /*
           SUM OF ITEM DISCOUNT RATES
           Stored here as fractional rates.
        */

        SUM(
            item_discount_amount
        ) AS total_discount,

        /*
           REVENUE AFTER ITEM-LEVEL DISCOUNT
        */

        SUM(
            quantity
            * unit_price
            * (1 - item_discount_amount)
        ) AS line_revenue,

        /*
           COST
        */

        SUM(
            quantity * cost_price
        ) AS line_cost

    FROM cleaned_items

    GROUP BY order_id

),

/*
   6. COMBINE ORDER HEADER + ITEM AGGREGATES
*/

combined AS (

    SELECT

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        o.order_id,
        o.customer_id,
        o.store_id,
        o.employee_id,

        o.order_datetime,
        o.order_date,

        o.shipping_date,
        o.delivery_date,
        o.estimated_delivery_date,

        o.order_discount_amount,

        o.shipping_cost,
        o.tax_amount,

        COALESCE(i.total_items, 0) AS total_items,
        COALESCE(i.total_quantity, 0) AS total_quantity,
        COALESCE(i.total_amount, 0.00) AS total_amount,
        COALESCE(i.total_cost, 0.00) AS total_cost,
        COALESCE(i.total_discount, 0.00) AS total_discount,

        COALESCE(i.line_revenue, 0.00) AS line_revenue,
        COALESCE(i.line_cost, 0.00) AS line_cost

    FROM order_header o

    LEFT JOIN order_item_aggregates i
        ON o.order_id = i.order_id

),

/*
   7. ORDER-SPECIFIC DERIVED ATTRIBUTES
*/

derived AS (

    SELECT

        c.*,

        /*
           ORDER HOUR
        */

        EXTRACT(
            HOUR FROM c.order_datetime
        ) AS order_hour,

        /*
           TIME OF DAY
        */

        CASE
            WHEN EXTRACT(HOUR FROM c.order_datetime) >= 5
                 AND EXTRACT(HOUR FROM c.order_datetime) < 12
                THEN 'Morning'

            WHEN EXTRACT(HOUR FROM c.order_datetime) >= 12
                 AND EXTRACT(HOUR FROM c.order_datetime) < 17
                THEN 'Afternoon'

            WHEN EXTRACT(HOUR FROM c.order_datetime) >= 17
                 AND EXTRACT(HOUR FROM c.order_datetime) < 22
                THEN 'Evening'

            ELSE 'Night'
        END AS order_time_of_day,

        /*
           CALENDAR ATTRIBUTES
        */

        WEEK(c.order_date) AS order_week,

        MONTH(c.order_date) AS order_month,

        QUARTER(c.order_date) AS order_quarter,

        YEAR(c.order_date) AS order_year,

        /*
           PROFIT AMOUNT

           line_revenue is already net of ITEM discount.

           The ORDER discount is then applied multiplicatively.
        */

        (
            c.line_revenue
            * (1 - c.order_discount_amount)
        )
        - c.line_cost
        - c.shipping_cost
        - c.tax_amount
        AS profit_amount,

        /*
           PROFIT MARGIN
        */

        CASE
            WHEN c.line_revenue > 0
            THEN
                (
                    (
                        (
                            c.line_revenue
                            * (1 - c.order_discount_amount)
                        )
                        - c.line_cost
                        - c.shipping_cost
                        - c.tax_amount
                    )
                    / c.line_revenue
                ) * 100
            ELSE NULL
        END AS profit_margin_percentage,

        /*
           PROCESSING DAYS
        */

        DATEDIFF(
            DAY,
            c.order_date,
            c.shipping_date
        ) AS processing_days,

        /*
           SHIPPING DAYS
        */

        DATEDIFF(
            DAY,
            c.shipping_date,
            c.delivery_date
        ) AS shipping_days,

        /*
           DELIVERY STATUS
        */

        CASE
            WHEN c.delivery_date IS NOT NULL
                 AND c.delivery_date <= c.estimated_delivery_date
                THEN 'On Time'

            WHEN c.delivery_date IS NOT NULL
                 AND c.delivery_date > c.estimated_delivery_date
                THEN 'Delayed'

            WHEN c.delivery_date IS NULL
                 AND CURRENT_DATE() > c.estimated_delivery_date
                THEN 'Potentially Delayed'

            ELSE 'In Transit'
        END AS delivery_status

    FROM combined c

),

/*
   8. DEDUPLICATION

   Natural key = order_id

   Keep the most recently modified/source version.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            CASE
                WHEN order_id IS NOT NULL
                    THEN order_id

                ELSE CONCAT(
                    '_NULL_',
                    SOURCE_FILE,
                    '_',
                    ROW_NUMBER
                )
            END

        ORDER BY
            order_datetime DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/*
   FINAL SILVER ORDERS TABLE
*/

SELECT *

FROM deduplicated