{{ config(
    materialized='view'
) }}

SELECT

    fmp.campaign_key,

    dmc.campaign_id,

    dmc.campaign_type,

    fmp.date_key,

    dd.full_date,

    fmp.total_sales_influenced,

    fmp.total_campaign_cost,

    fmp.roi

FROM {{ ref('Fact_MarketingPerformance') }} fmp

LEFT JOIN {{ ref('Dim_MarketingCampaign') }} dmc

    ON fmp.campaign_key =
       dmc.campaign_key

LEFT JOIN {{ ref('Dim_date') }} dd

    ON fmp.date_key =
       dd.date_key