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
    FROM {{ ref('stg_bronze__campaigns_data') }}

),

/*
   1. FLATTEN THE CAMPAIGNS ARRAY
*/

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        campaign.value AS campaign_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:campaigns_data
    ) AS campaign

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
           CAMPAIGN ID
        */

        NULLIF(
            TRIM(
                campaign_data:campaign_id::VARCHAR
            ),
            ''
        ) AS campaign_id,


        /*
           CAMPAIGN NAME
           Trim whitespace
           Remove unwanted characters
           Standardize capitalization
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    campaign_data:campaign_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''&-]',
                ''
            )
        ) AS campaign_name,


        /*
           START DATE
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    campaign_data:start_date::VARCHAR
                ),
                ''
            )
        ) AS start_date,


        /*
           END DATE
        */

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    campaign_data:end_date::VARCHAR
                ),
                ''
            )
        ) AS end_date,


        /*
           BUDGET
           Parse currency strings
           Example:
           $24,005.75 -> 24005.75
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        campaign_data:budget::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS budget,


        /*
           TOTAL COST
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        campaign_data:total_cost::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS total_cost,


        /*
           TOTAL REVENUE
        */

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        campaign_data:total_revenue::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS total_revenue,


        /*
           ROI CALCULATION

           Only cast the existing source value
           from string to numeric.

           ROI itself is NOT calculated in Silver.
        */

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    campaign_data:roi_calculation::VARCHAR
                ),
                ''
            ),
            18,
            4
        ) AS roi_calculation,


        /*
           DEMOGRAPHICS

           Keep the source demographic information
           available for audience segmentation.
        */

        campaign_data:demographics AS demographics,


        /*
           LAST MODIFIED DATE
        */

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    campaign_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

/*
   3. CAMPAIGN-SPECIFIC DERIVED ATTRIBUTES
*/

derived AS (

    SELECT

        c.*,


        /*
           CAMPAIGN DURATION

           Duration in days between start and end dates.
        */

        CASE
            WHEN c.start_date IS NOT NULL
                 AND c.end_date IS NOT NULL
            THEN DATEDIFF(
                DAY,
                c.start_date,
                c.end_date
            )
            ELSE NULL
        END AS campaign_duration_days,


        /*
           AUDIENCE SEGMENT

           The problem statement requires
           demographic-based segmentation.
           
           Until the exact demographic fields/rules
           are confirmed, preserve the source data.
        */

        NULL::VARCHAR AS audience_segment

    FROM cleaned c

),

/*
   4. DEDUPLICATION

   Natural key = campaign_id

   Keep the most recently modified record.
*/

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            CASE
                WHEN campaign_id IS NOT NULL
                    THEN campaign_id

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
   FINAL SILVER CAMPAIGN TABLE
*/

SELECT *

FROM deduplicated