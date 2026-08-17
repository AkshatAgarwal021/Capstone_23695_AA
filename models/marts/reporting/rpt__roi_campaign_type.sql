{{ config(
    materialized='view'
) }}

SELECT

    dmc.campaign_id,

    dmc.campaign_name,

    COUNT(
        DISTINCT fmp.campaign_key
    ) AS campaign_count,

    SUM(
        fmp.total_sales_influenced
    ) AS total_sales_influenced,

    SUM(
        fmp.total_campaign_cost
    ) AS total_campaign_cost,

    AVG(
        fmp.roi
    ) AS average_roi,

    CASE

        WHEN SUM(
            fmp.total_campaign_cost
        ) > 0

        THEN
            (
                SUM(
                    fmp.total_sales_influenced
                )
                -
                SUM(
                    fmp.total_campaign_cost
                )
            )
            /
            SUM(
                fmp.total_campaign_cost
            )
            * 100

        ELSE NULL

    END AS calculated_roi

FROM {{ ref('Fact_MarketingPerformance') }} fmp

LEFT JOIN {{ ref('Dim_MarketingCampaign') }} dmc

    ON fmp.campaign_key =
       dmc.campaign_key

GROUP BY

    dmc.campaign_id,
    dmc.campaign_name