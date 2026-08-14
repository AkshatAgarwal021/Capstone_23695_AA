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
    FROM {{ ref('stg_bronze__employees_data') }}

),

/*
   1. FLATTEN THE EMPLOYEES ARRAY
*/

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        employee.value AS employee_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:employees_data
    ) AS employee

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
           EMPLOYEE ID
        */

        NULLIF(
            TRIM(
                employee_data:employee_id::VARCHAR
            ),
            ''
        ) AS employee_id,


        /*
           FIRST NAME

           Trim whitespace
           Remove unwanted characters
           Standardize capitalization
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    employee_data:first_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS first_name,


        /*
           LAST NAME
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    employee_data:last_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS last_name,


        /*
           EMAIL

           Normalize to lowercase.
           Invalid emails become NULL.
        */

        CASE
            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        employee_data:email::VARCHAR
                    )
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN LOWER(
                TRIM(
                    employee_data:email::VARCHAR
                )
            )
            ELSE NULL
        END AS email,


        /*
           INVALID EMAIL FLAG
        */

        CASE
            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        employee_data:email::VARCHAR
                    )
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_email_flag,


        /*
           PHONE

           Keep digits only.
           Validate 10-digit format.
        */

        CASE
            WHEN REGEXP_LIKE(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                '^[0-9]{10}$'
            )
            THEN REGEXP_REPLACE(
                TRIM(
                    employee_data:phone::VARCHAR
                ),
                '[^0-9]',
                ''
            )
            ELSE NULL
        END AS phone,


        /*
           INVALID PHONE FLAG
        */

        CASE
            WHEN REGEXP_LIKE(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                '^[0-9]{10}$'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_phone_flag,


        /*
           JOB TITLE
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    employee_data:job_title::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS job_title,


        /*
           DEPARTMENT
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    employee_data:department::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS department,


        /*
           STORE ID
        */

        NULLIF(
            TRIM(
                employee_data:store_id::VARCHAR
            ),
            ''
        ) AS store_id,


        /*
           HIRE DATE

           Standardized to DATE.
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    employee_data:hire_date::VARCHAR
                ),
                ''
            )
        ) AS hire_date,


        /*
           SALARY

           Parse monetary values such as:
           $24,005.75
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:salary::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS salary,


        /*
           LAST MODIFIED DATE
        */

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    employee_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. EMPLOYEE-SPECIFIC BASIC DERIVED ATTRIBUTES

   No business-specific transformation is required
   by the problem statement for Employees.

   We therefore keep this limited to useful
   standardized attributes.
*/

derived AS (

    SELECT

        e.*,

        /*
           FULL NAME
        */

        TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(e.first_name, ''),
                NULLIF(e.last_name, '')
            )
        ) AS full_name

    FROM cleaned e

),

/*
   4. DEDUPLICATION

   Natural key = employee_id

   Keep the most recently modified version.

   Metadata provides deterministic tie-breakers.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            CASE
                WHEN employee_id IS NOT NULL
                    THEN employee_id

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
   FINAL SILVER EMPLOYEE TABLE
*/

SELECT *

FROM deduplicated