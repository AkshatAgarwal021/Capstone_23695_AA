{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_history_key',
    on_schema_change='sync_all_columns'
) }}

WITH source_files AS (

    SELECT
        SOURCE_FILE,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__products_data') }}

    {% if is_incremental() %}

        WHERE TRY_TO_DATE(
            REGEXP_SUBSTR(
                SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) > (

            SELECT COALESCE(
                MAX(source_snapshot_date),
                DATE '1900-01-01'
            )
            FROM {{ this }}

        )

    {% endif %}

),

/*
   1. FLATTEN PRODUCT SNAPSHOT DATA
*/

flattened AS (

    SELECT

        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS source_snapshot_date,

        product.value AS product_data

    FROM source_files,

    LATERAL FLATTEN(
        INPUT => RAW_DATA:products_data
    ) AS product

),

/*
   2. EXTRACT + CLEAN PRODUCT HISTORY
*/

cleaned AS (

    SELECT

        /*
           HISTORY KEY

           Product + Snapshot Date

           Store is NOT sourced from Product JSON.
        */

        {{ dbt_utils.generate_surrogate_key([
            'product_data:product_id::VARCHAR',
            'source_snapshot_date'
        ]) }} AS product_history_key,


        /*
           SOURCE METADATA
        */

        SOURCE_FILE,
        source_snapshot_date,
        LOADED_AT,
        BATCH_ID,


        /*
           PRODUCT ID
        */

        NULLIF(
            TRIM(
                product_data:product_id::VARCHAR
            ),
            ''
        ) AS product_id,


        /*
           PRODUCT NAME
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    product_data:name::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_name,


        /*
           FULL PRODUCT DESCRIPTION
        */

        TRIM(
            CONCAT_WS(
                ' - ',

                NULLIF(
                    TRIM(
                        product_data:name::VARCHAR
                    ),
                    ''
                ),

                NULLIF(
                    TRIM(
                        product_data:short_description::VARCHAR
                    ),
                    ''
                ),

                NULLIF(
                    TRIM(
                        product_data:technical_specs::VARCHAR
                    ),
                    ''
                )

            )
        ) AS full_description,


        /*
           DESCRIPTION COMPONENTS
        */

        TRIM(
            REGEXP_REPLACE(
                product_data:short_description::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%-]',
                ''
            )
        ) AS short_description,

        TRIM(
            REGEXP_REPLACE(
                product_data:technical_specs::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%_=-]',
                ''
            )
        ) AS technical_specs,


        /*
           PRODUCT HIERARCHY
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    product_data:category::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS category,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    product_data:subcategory::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS subcategory,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    product_data:product_line::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_line,


        /*
           PRODUCT ATTRIBUTES
        */

        INITCAP(
            TRIM(
                product_data:brand::VARCHAR
            )
        ) AS brand,

        INITCAP(
            TRIM(
                product_data:color::VARCHAR
            )
        ) AS color,

        INITCAP(
            TRIM(
                product_data:size::VARCHAR
            )
        ) AS size,


        /*
           MONEY
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        product_data:unit_price::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS unit_price,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        product_data:cost_price::VARCHAR
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
           INVENTORY
        */

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    product_data:stock_quantity::VARCHAR
                ),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    product_data:reorder_level::VARCHAR
                ),
                ''
            )
        ) AS reorder_level,


        /*
           SUPPLIER
        */

        NULLIF(
            TRIM(
                product_data:supplier_id::VARCHAR
            ),
            ''
        ) AS supplier_id,


        /*
           OTHER PRODUCT ATTRIBUTES
        */

        TRIM(
            product_data:dimensions::VARCHAR
        ) AS dimensions,

        TRIM(
            product_data:warranty_period::VARCHAR
        ) AS warranty_period,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    LOWER(
                        TRIM(
                            product_data:weight::VARCHAR
                        )
                    ),
                    '[^0-9.\-]',
                    ''
                ),
                ''
            ),
            10,
            2
        ) AS weight_kg,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    product_data:launch_date::VARCHAR
                ),
                ''
            )
        ) AS launch_date,

        COALESCE(
            product_data:is_featured::BOOLEAN,
            FALSE
        ) AS is_featured,


        /*
           SOURCE MODIFICATION DATE
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    product_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. DERIVED ATTRIBUTES
*/

derived AS (

    SELECT

        c.*,

        TRIM(
            CONCAT_WS(
                ' > ',

                NULLIF(c.category, ''),
                NULLIF(c.subcategory, ''),
                NULLIF(c.product_line, '')

            )
        ) AS product_hierarchy,

        CASE

            WHEN c.unit_price > 0

            THEN (
                (c.unit_price - c.cost_price)
                / c.unit_price
            ) * 100

            ELSE NULL

        END AS profit_margin_percentage,

        CASE

            WHEN c.stock_quantity IS NULL
              OR c.reorder_level IS NULL
                THEN NULL

            WHEN c.stock_quantity < c.reorder_level
                THEN TRUE

            ELSE FALSE

        END AS low_stock_flag

    FROM cleaned c

),

/*
   4. DEDUPLICATION

   One product per snapshot date.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            product_id,
            source_snapshot_date

        ORDER BY
            SOURCE_FILE DESC,
            LOADED_AT DESC

    ) = 1

)

/*
   FINAL SILVER PRODUCT HISTORY TABLE
*/

SELECT

    product_history_key,

    SOURCE_FILE,
    source_snapshot_date,
    LOADED_AT,
    BATCH_ID,

    product_id,

    product_name,
    full_description,
    short_description,
    technical_specs,

    category,
    subcategory,
    product_line,
    product_hierarchy,

    brand,
    color,
    size,

    unit_price,
    cost_price,
    profit_margin_percentage,

    stock_quantity,
    reorder_level,
    low_stock_flag,

    supplier_id,

    dimensions,
    weight_kg,
    warranty_period,

    is_featured,
    launch_date,
    last_modified_date

FROM deduplicated