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
           US FORMAT: (XXX) XXX-XXXX
        */

        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            employee_data:phone::VARCHAR
                        ),
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
        */

        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN FALSE

            ELSE TRUE

        END AS invalid_phone_flag,


        /*
           JOB TITLE
           Source JSON field = role
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    employee_data:role::VARCHAR
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
           Source JSON field = work_location
        */

        NULLIF(
            TRIM(
                employee_data:work_location::VARCHAR
            ),
            ''
        ) AS store_id,


        /*
           HIRE DATE
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
        */

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    employee_data:salary::VARCHAR
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
   3. DERIVED ATTRIBUTE
*/

derived AS (

    SELECT

        e.*,

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