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
    FROM {{ ref('stg_bronze__customers_data') }}
 
),
 
/*
   1. FLATTEN THE CUSTOMERS ARRAY
*/
 
flattened AS (
 
    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
 
        customer.value AS customer_data
 
    FROM source_data s,
 
    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:customers_data
    ) customer
 
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
           CUSTOMER ID
        */
 
        NULLIF(
            TRIM(customer_data:customer_id::VARCHAR),
            ''
        ) AS customer_id,
 
 
        /*
           NAME
           Trim whitespace
           Remove unwanted characters
           Standardize capitalization
        */
 
        INITCAP(
            REGEXP_REPLACE(
                TRIM(customer_data:first_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS first_name,
 
        INITCAP(
            REGEXP_REPLACE(
                TRIM(customer_data:last_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS last_name,
 
 
        /*
           EMAIL
           Validate + normalize
        */
 
        CASE
            WHEN REGEXP_LIKE(
                LOWER(TRIM(customer_data:email::VARCHAR)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN LOWER(TRIM(customer_data:email::VARCHAR))
            ELSE NULL
        END AS email,
 
        CASE
            WHEN REGEXP_LIKE(
                LOWER(TRIM(customer_data:email::VARCHAR)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_email_flag,
 
 
        /*
           PHONE
           Normalize to US format:
           (XXX) XXX-XXXX

           Supports:
           10-digit numbers
           11-digit numbers beginning with 1
        */
 
        CASE
 
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
 
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )
 
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
 
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
 
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    8,
                    4
                )
            )
 
            ELSE NULL
 
        END AS phone,
 
 
        /*
           INVALID PHONE FLAG
           
           Valid:
           10 digits
           11 digits beginning with 1
        */
 
        CASE
 
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
 
            THEN FALSE
 
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
 
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
 
            THEN FALSE
 
            ELSE TRUE
 
        END AS invalid_phone_flag,
 
 
        /*
           ADDRESS
           Standardize individual components
        */
 
        INITCAP(
            TRIM(customer_data:address:street::VARCHAR)
        ) AS street,
 
        INITCAP(
            TRIM(customer_data:address:city::VARCHAR)
        ) AS city,
 
        UPPER(
            TRIM(customer_data:address:state::VARCHAR)
        ) AS state,
 
        UPPER(
            TRIM(customer_data:address:country::VARCHAR)
        ) AS country,
 
        TRIM(
            customer_data:address:zip_code::VARCHAR
        ) AS zip_code,
 
 
        /*
           STANDARDIZED ADDRESS
        */
 
        CONCAT_WS(
            ', ',
            NULLIF(
                INITCAP(TRIM(customer_data:address:street::VARCHAR)),
                ''
            ),
            NULLIF(
                INITCAP(TRIM(customer_data:address:city::VARCHAR)),
                ''
            ),
            NULLIF(
                UPPER(TRIM(customer_data:address:state::VARCHAR)),
                ''
            ),
            NULLIF(
                TRIM(customer_data:address:zip_code::VARCHAR),
                ''
            ),
            NULLIF(
                UPPER(TRIM(customer_data:address:country::VARCHAR)),
                ''
            )
        ) AS standardized_address,
 
 
        /*
           CUSTOMER ATTRIBUTES
        */
 
        UPPER(
            TRIM(customer_data:income_bracket::VARCHAR)
        ) AS income_bracket,
 
        INITCAP(
            TRIM(customer_data:occupation::VARCHAR)
        ) AS occupation,
 
        UPPER(
            TRIM(customer_data:loyalty_tier::VARCHAR)
        ) AS loyalty_tier,
 
        UPPER(
            TRIM(customer_data:preferred_communication::VARCHAR)
        ) AS preferred_communication,
 
        UPPER(
            TRIM(customer_data:preferred_payment_method::VARCHAR)
        ) AS preferred_payment_method,
 
        COALESCE(
            customer_data:marketing_opt_in::BOOLEAN,
            FALSE
        ) AS marketing_opt_in,
 
 
        /*
           DATES
           Standardized to DATE
        */
 
        TRY_TO_DATE(
            NULLIF(
                TRIM(customer_data:birth_date::VARCHAR),
                ''
            )
        ) AS birth_date,
 
        TRY_TO_DATE(
            NULLIF(
                TRIM(customer_data:registration_date::VARCHAR),
                ''
            )
        ) AS registration_date,
 
        TRY_TO_DATE(
            NULLIF(
                TRIM(customer_data:last_purchase_date::VARCHAR),
                ''
            )
        ) AS last_purchase_date,
 
        TRY_TO_DATE(
            NULLIF(
                TRIM(customer_data:last_modified_date::VARCHAR),
                ''
            )
        ) AS last_modified_date,
 
 
        /*
           NUMERIC VALUES
        */
 
        COALESCE(
            TRY_TO_NUMBER(
                customer_data:total_purchases::VARCHAR
            ),
            0
        ) AS total_purchases,
 
        COALESCE(
            TRY_TO_DECIMAL(
                customer_data:total_spend::VARCHAR,
                18,
                2
            ),
            0.00
        ) AS total_spend
 
    FROM flattened
 
),
 
/*
   3. CUSTOMER-SPECIFIC DERIVED ATTRIBUTES
*/
 
derived AS (
 
    SELECT
 
        c.*,
 
 
        /*
           FULL NAME
           FirstName || ' ' || LastName
        */
 
        TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(c.first_name, ''),
                NULLIF(c.last_name, '')
            )
        ) AS full_name,
 
 
        /*
           CUSTOMER AGE
        */
 
        CASE
            WHEN c.birth_date IS NOT NULL
            THEN DATEDIFF(
                YEAR,
                c.birth_date,
                CURRENT_DATE()
            )
            ELSE NULL
        END AS customer_age
 
    FROM cleaned c
 
),
 
/*
   4. CUSTOMER SEGMENT
   Required non-overlapping bands:
      Young       = 18-35
      Middle-aged = 36-55
      Senior      = 56+
*/
 
segmented AS (
 
    SELECT
 
        d.*,
 
        CASE
            WHEN customer_age BETWEEN 18 AND 35
                THEN 'Young'
 
            WHEN customer_age BETWEEN 36 AND 55
                THEN 'Middle-aged'
 
            WHEN customer_age >= 56
                THEN 'Senior'
 
            ELSE NULL
        END AS customer_segment
 
    FROM derived d
 
),
 
/*
   5. DEDUPLICATION
   Natural key = customer_id
 
   Keep the most recently modified version.
 
   Metadata is used as deterministic tie-breakers.
*/
 
deduplicated AS (
 
    SELECT *
 
    FROM segmented
 
    QUALIFY ROW_NUMBER() OVER (
 
        PARTITION BY
            CASE
                WHEN customer_id IS NOT NULL
                    THEN customer_id
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
   FINAL SILVER CUSTOMER TABLE
*/
 
SELECT *
FROM deduplicated