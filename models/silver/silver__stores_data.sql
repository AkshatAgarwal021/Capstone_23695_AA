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
    FROM {{ ref('stg_bronze__stores_data') }}

),

/*
   1. FLATTEN THE STORES ARRAY
*/

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        store.value AS store_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:stores_data
    ) AS store

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
           STORE ID
        */

        NULLIF(
            TRIM(
                store_data:store_id::VARCHAR
            ),
            ''
        ) AS store_id,


        /*
           STORE NAME

           Prefer store_name.
           Fall back to name if necessary.

           Pascal Case:
           "denver downtown" -> "DenverDowntown"
        */

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    COALESCE(
                        store_data:store_name::VARCHAR,
                        store_data:name::VARCHAR
                    )
                )
            ),
            '[^A-Za-z0-9]+',
            ''
        ) AS store_name,


        /*
           ADDRESS
           Support nested address structure.
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    COALESCE(
                        store_data:address:street::VARCHAR,
                        store_data:street::VARCHAR
                    )
                ),
                '[^A-Za-z0-9 ''#.,/-]',
                ''
            )
        ) AS street,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    COALESCE(
                        store_data:address:city::VARCHAR,
                        store_data:city::VARCHAR
                    )
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS city,

        UPPER(
            TRIM(
                COALESCE(
                    store_data:address:state::VARCHAR,
                    store_data:state::VARCHAR
                )
            )
        ) AS state,

        UPPER(
            TRIM(
                COALESCE(
                    store_data:address:country::VARCHAR,
                    store_data:country::VARCHAR
                )
            )
        ) AS country,


        /*
           POSTAL CODE

           Support zip_code or postal_code.
           Accept:
             12345
             12345-6789

           Invalid values become NULL.
        */

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )

            THEN TRIM(
                COALESCE(
                    store_data:address:zip_code::VARCHAR,
                    store_data:address:postal_code::VARCHAR,
                    store_data:zip_code::VARCHAR,
                    store_data:postal_code::VARCHAR
                )
            )

            ELSE NULL

        END AS postal_code,


        /*
           POSTAL CODE VALIDATION FLAG
        */

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )

            THEN FALSE

            ELSE TRUE

        END AS invalid_postal_code_flag,


        /*
           STANDARDIZED ADDRESS
        */

        CONCAT_WS(
            ', ',

            NULLIF(
                INITCAP(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE(
                                store_data:address:street::VARCHAR,
                                store_data:street::VARCHAR
                            )
                        ),
                        '[^A-Za-z0-9 ''#.,/-]',
                        ''
                    )
                ),
                ''
            ),

            NULLIF(
                INITCAP(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE(
                                store_data:address:city::VARCHAR,
                                store_data:city::VARCHAR
                            )
                        ),
                        '[^A-Za-z0-9 ''-]',
                        ''
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        COALESCE(
                            store_data:address:state::VARCHAR,
                            store_data:state::VARCHAR
                        )
                    )
                ),
                ''
            ),

            NULLIF(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        COALESCE(
                            store_data:address:country::VARCHAR,
                            store_data:country::VARCHAR
                        )
                    )
                ),
                ''
            )

        ) AS standardized_address,


        /*
           STORE SIZE IN SQUARE FEET
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(
                        store_data:size_sq_ft::VARCHAR
                    ),
                    ''
                ),
                18,
                2
            ),
            0
        ) AS size_sq_ft,


        /*
           OPENING DATE
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    store_data:opening_date::VARCHAR
                ),
                ''
            )
        ) AS opening_date,


        /*
           SALES TARGET
           Parse currency strings.
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(
                            store_data:sales_target::VARCHAR
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
        ) AS sales_target,


        /*
           CURRENT SALES
           Parse currency strings.
        */

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(
                            store_data:current_sales::VARCHAR
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
        ) AS current_sales,


        /*
           EMPLOYEE COUNT
        */

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(
                        store_data:employee_count::VARCHAR
                    ),
                    ''
                )
            ),
            0
        ) AS employee_count,


        /*
           LAST MODIFIED DATE

           Keep timestamp precision for deduplication.
        */

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    store_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. STORE-SPECIFIC DERIVED ATTRIBUTES
*/

derived AS (

    SELECT

        s.*,


        /*
           STORE SIZE CATEGORY

           < 5000       = Small
           5000-10000   = Medium
           > 10000      = Large
        */

        CASE

            WHEN s.size_sq_ft < 5000
                THEN 'Small'

            WHEN s.size_sq_ft >= 5000
                 AND s.size_sq_ft <= 10000
                THEN 'Medium'

            WHEN s.size_sq_ft > 10000
                THEN 'Large'

            ELSE NULL

        END AS store_size_category,


        /*
           STORE AGE IN YEARS

           Do not calculate negative age
           for a future opening date.
        */

        CASE

            WHEN s.opening_date IS NOT NULL
                 AND s.opening_date <= CURRENT_DATE()

            THEN DATEDIFF(
                YEAR,
                s.opening_date,
                CURRENT_DATE()
            )

            ELSE NULL

        END AS store_age_years,


        /*
           SALES TARGET ACHIEVEMENT %

           Guard against zero target.
        */

        CASE

            WHEN s.sales_target > 0

            THEN (
                s.current_sales
                / s.sales_target
            ) * 100

            ELSE NULL

        END AS sales_target_achievement_percentage,


        /*
           REVENUE PER SQUARE FOOT

           Guard against zero square footage.
        */

        CASE

            WHEN s.size_sq_ft > 0

            THEN
                s.current_sales
                / s.size_sq_ft

            ELSE NULL

        END AS revenue_per_sq_ft,


        /*
           EMPLOYEE EFFICIENCY

           Guard against zero employees.
        */

        CASE

            WHEN s.employee_count > 0

            THEN
                s.current_sales
                / s.employee_count

            ELSE NULL

        END AS employee_efficiency

    FROM cleaned s

),

/*
   4. PERFORMANCE ISSUE FLAG

   Achievement below 90% = performance issue.
*/

performance_flagged AS (

    SELECT

        d.*,

        CASE

            WHEN d.sales_target_achievement_percentage < 90
                THEN TRUE

            ELSE FALSE

        END AS performance_issue_flag

    FROM derived d

),

/*
   5. DEDUPLICATION

   Natural key = store_id

   Keep the most recently modified record.
*/

deduplicated AS (

    SELECT *

    FROM performance_flagged

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY

            CASE

                WHEN store_id IS NOT NULL
                    THEN store_id

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
   FINAL SILVER STORE TABLE
*/

SELECT *

FROM deduplicated