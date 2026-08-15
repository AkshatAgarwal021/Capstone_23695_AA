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
    FROM {{ ref('stg_bronze__suppliers_data') }}

),

/*
   1. FLATTEN THE SUPPLIERS ARRAY
*/

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        supplier.value AS supplier_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:suppliers_data
    ) AS supplier

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
           SUPPLIER ID
        */

        NULLIF(
            TRIM(
                supplier_data:supplier_id::VARCHAR
            ),
            ''
        ) AS supplier_id,


        /*
           SUPPLIER NAME
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:supplier_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''&.,/-]',
                ''
            )
        ) AS supplier_name,


        /*
           CONTACT PERSON

           Actual JSON path:
           contact_information.contact_person
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:contact_information:contact_person::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS contact_name,


        /*
           EMAIL

           Actual JSON path:
           contact_information.email

           Invalid email becomes NULL.
        */

        CASE
            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        supplier_data:contact_information:email::VARCHAR
                    )
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN LOWER(
                TRIM(
                    supplier_data:contact_information:email::VARCHAR
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
                        supplier_data:contact_information:email::VARCHAR
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

           Actual JSON path:
           contact_information.phone

           Standard US format:
           (XXX) XXX-XXXX
        */

        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                            supplier_data:contact_information:phone::VARCHAR
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
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
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
           RAW ADDRESS

           Actual JSON path:
           contact_information.address
        */

        TRIM(
            supplier_data:contact_information:address::VARCHAR
        ) AS raw_address,


        /*
           STREET

           Address format:
           street, city, state, zip, country

           Example:
           808 James Fort,
           West Kristine,
           KY,
           41934,
           USA
        */

        TRIM(
            SPLIT_PART(
                supplier_data:contact_information:address::VARCHAR,
                ',',
                1
            )
        ) AS street,


        /*
           CITY
        */

        INITCAP(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    2
                )
            )
        ) AS city,


        /*
           STATE
        */

        UPPER(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    3
                )
            )
        ) AS state,


        /*
           POSTAL CODE
        */

        CASE
            WHEN REGEXP_LIKE(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    4
                )
            )
            ELSE NULL
        END AS postal_code,


        /*
           INVALID POSTAL CODE FLAG
        */

        CASE
            WHEN REGEXP_LIKE(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_postal_code_flag,


        /*
           COUNTRY
        */

        UPPER(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    5
                )
            )
        ) AS country,


        /*
           STANDARDIZED ADDRESS
        */

        CONCAT_WS(
            ', ',

            NULLIF(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        1
                    )
                ),
                ''
            ),

            NULLIF(
                INITCAP(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            2
                        )
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            3
                        )
                    )
                ),
                ''
            ),

            NULLIF(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            5
                        )
                    )
                ),
                ''
            )

        ) AS standardized_address,


        /*
           PAYMENT TERMS
        */

        INITCAP(
            TRIM(
                supplier_data:payment_terms::VARCHAR
            )
        ) AS payment_terms,


        /*
           SUPPLIER TYPE
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:supplier_type::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS supplier_type,


        /*
           LAST MODIFIED DATE
        */

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    supplier_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. DEDUPLICATION

   Natural key = supplier_id

   Keep the most recently modified version.
*/

deduplicated AS (

    SELECT *

    FROM cleaned

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY

            CASE
                WHEN supplier_id IS NOT NULL
                    THEN supplier_id

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
   FINAL SILVER SUPPLIER TABLE
*/

SELECT *

FROM deduplicated