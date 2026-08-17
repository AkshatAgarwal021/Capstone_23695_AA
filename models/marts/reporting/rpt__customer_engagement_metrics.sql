{{ config(
    materialized='view'
) }}

SELECT

    fmp.campaign_key,

    dmc.campaign_id,

    dmc.campaign_type,

    MIN(
        fmp.date_key
    ) AS campaign_start_date_key,

    MAX(
        fmp.date_key
    ) AS campaign_end_date_key,

    SUM(
        fmp.new_customers_acquired
    ) AS new_customers_acquired,

    AVG(
        fmp.repeat_purchase_rate
    ) AS repeat_purchase_rate,

    AVG(
        fmp.total_sales_influenced
    ) AS average_daily_sales_influenced,

    SUM(
        fmp.total_sales_influenced
    ) AS total_sales_influenced

FROM {{ ref('Fact_MarketingPerformance') }} fmp

LEFT JOIN {{ ref('Dim_MarketingCampaign') }} dmc

    ON fmp.campaign_key =
       dmc.campaign_key

GROUP BY

    fmp.campaign_key,

    dmc.campaign_id,

    dmc.campaign_type