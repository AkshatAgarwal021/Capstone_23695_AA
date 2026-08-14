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
    FROM {{ ref('stg_bronze__products_data') }}

),

/*
   1. FLATTEN THE PRODUCTS ARRAY
*/

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        product.value AS product_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:products_data
    ) product

),

/*
   2. EXTRACT + CLEAN + STANDARDIZE
*/

cleaned AS (

    SELECT

        /*
           AUDIT / LINEAGE METADATA
        */

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,


        /*
           PRODUCT ID
        */

        NULLIF(
            TRIM(product_data:product_id::VARCHAR),
            ''
        ) AS product_id,


        /*
           PRODUCT NAME
           Trim whitespace
           Remove unwanted characters
           Standardize to Pascal Case
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:name::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_name,


        /*
           SHORT DESCRIPTION
           Trim whitespace
           Remove unwanted special characters
        */

        TRIM(
            REGEXP_REPLACE(
                product_data:short_description::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%-]',
                ''
            )
        ) AS short_description,


        /*
           TECHNICAL SPECIFICATIONS
           Trim whitespace
           Remove unwanted special characters
        */

        TRIM(
            REGEXP_REPLACE(
                product_data:technical_specs::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%_=-]',
                ''
            )
        ) AS technical_specs,


        /*
           CATEGORY
           Standardize to Pascal Case
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:category::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS category,


        /*
           SUBCATEGORY
           Standardize to Pascal Case
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:subcategory::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS subcategory,


        /*
           PRODUCT LINE
           Standardize to Pascal Case
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:product_line::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_line,


        /*
           UNIT PRICE
           Parse currency strings such as:
           $24,005.75
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(product_data:unit_price::VARCHAR),
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
           Parse currency strings
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(product_data:cost_price::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS cost_price,


        /*
           STOCK QUANTITY
        */

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(product_data:stock_quantity::VARCHAR),
                    ''
                )
            ),
            0
        ) AS stock_quantity,


        /*
           REORDER LEVEL
        */

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(product_data:reorder_level::VARCHAR),
                    ''
                )
            ),
            0
        ) AS reorder_level,


        /*
           LAST MODIFIED DATE
           Standardized to DATE
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(product_data:last_modified_date::VARCHAR),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. PRODUCT-SPECIFIC DERIVED ATTRIBUTES
*/

derived AS (

    SELECT

        p.*,


        /*
           FULL PRODUCT DESCRIPTION

           name + short_description + technical_specs
        */

        CONCAT_WS(
            ' - ',
            NULLIF(TRIM(p.product_name), ''),
            NULLIF(TRIM(p.short_description), ''),
            NULLIF(TRIM(p.technical_specs), '')
        ) AS product_full_description,


        /*
           PRODUCT HIERARCHY

           category > subcategory > product_line
        */

        CONCAT_WS(
            ' > ',
            NULLIF(TRIM(p.category), ''),
            NULLIF(TRIM(p.subcategory), ''),
            NULLIF(TRIM(p.product_line), '')
        ) AS product_hierarchy,


        /*
           PROFIT MARGIN PERCENTAGE

           (unit_price - cost_price) / unit_price * 100

           Guard against division by zero.
        */

        CASE
            WHEN p.unit_price > 0
            THEN (
                (p.unit_price - p.cost_price)
                / p.unit_price
            ) * 100
            ELSE NULL
        END AS profit_margin_percentage,


        /*
           LOW-STOCK FLAG

           TRUE when stock is below reorder level.
        */

        CASE
            WHEN p.stock_quantity < p.reorder_level
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag

    FROM cleaned p

),

/*
   4. DEDUPLICATION

   Natural key = product_id

   Keep the most recently modified record.

   Metadata provides deterministic tie-breakers.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            CASE
                WHEN product_id IS NOT NULL
                    THEN product_id
                ELSE CONCAT(
                    '_NULL_',
                    SOURCE_FILE,
                    '_',
                    ROW_NUMBER
                )
            END

        ORDER BY
            last_modified_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/*
   FINAL SILVER PRODUCT TABLE
*/

SELECT *
FROM deduplicated