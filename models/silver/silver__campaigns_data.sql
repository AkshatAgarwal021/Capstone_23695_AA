{{ config(materialized="table") }}

WITH
    source_data AS (

        SELECT
            source_file,
            row_number,
            raw_data,
            loaded_at,
            batch_id

        FROM {{ ref("stg_bronze__campaigns_data") }}

    ),

    /*
       1. FLATTEN THE CAMPAIGNS ARRAY
    */

    flattened AS (

        SELECT

            s.source_file,
            s.row_number,
            s.loaded_at,
            s.batch_id,

            campaign.value AS campaign_data

        FROM
            source_data s,

            LATERAL FLATTEN(
                INPUT => s.raw_data:campaigns_data
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

            source_file,
            row_number,
            loaded_at,
            batch_id,


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
               CAMPAIGN TYPE

               This is the actual campaign_type
               provided in the Bronze JSON.

               DO NOT derive this from
               target_audience_segmentation.
            */

            INITCAP(
                REGEXP_REPLACE(
                    TRIM(
                        campaign_data:campaign_type::VARCHAR
                    ),
                    '[^A-Za-z0-9 ''&/-]',
                    ''
                )
            ) AS campaign_type,


            /*
               TARGET AUDIENCE

               Preserve the complete demographic description.
            */

            INITCAP(
                REGEXP_REPLACE(
                    TRIM(
                        campaign_data:target_audience::VARCHAR
                    ),
                    '[^A-Za-z0-9 ''&/-]',
                    ''
                )
            ) AS target_audience_segmentation,


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

               Parse currency strings.
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

               Cast the source value to numeric.
               Do not calculate ROI here.
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

               Number of days between start and end dates.
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

            END AS campaign_duration_days

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
                        source_file,
                        '_',
                        row_number
                    )

                END

            ORDER BY

                last_modified_date DESC NULLS LAST,
                loaded_at DESC,
                source_file DESC,
                row_number DESC

        ) = 1

    )

/*
   FINAL SILVER CAMPAIGN TABLE
*/

SELECT *

FROM deduplicated