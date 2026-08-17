{{ config(
    materialized='table'
) }}

WITH campaigns AS (

    SELECT

        campaign_id,
        campaign_name,
        campaign_type,
        target_audience_segmentation,
        budget,
        campaign_duration_days,
        roi_calculation,
        start_date,
        end_date

    FROM {{ ref('silver__campaigns_data') }}

),

final AS (

    SELECT

        /*
           SURROGATE KEY

           Generated from the natural Campaign ID
           using dbt_utils.
        */

        {{ dbt_utils.generate_surrogate_key([
            'campaign_id'
        ]) }} AS campaign_key,


        /*
           NATURAL KEY
        */

        campaign_id,


        /*
           CAMPAIGN NAME
        */

        campaign_name,


        /*
           CAMPAIGN TYPE

           Actual campaign type supplied by
           the source JSON.

           This is NOT the target audience.
        */

        campaign_type,


        /*
           TARGET AUDIENCE
        */

        target_audience_segmentation
            AS target_audience_segment,


        /*
           CAMPAIGN BUDGET
        */

        budget,


        /*
           CAMPAIGN DURATION
        */

        campaign_duration_days
            AS duration,


        /*
           SOURCE ROI
        */

        roi_calculation
            AS roi,


        /*
           CAMPAIGN DATES
        */

        start_date,
        end_date

    FROM campaigns

)

SELECT *

FROM final