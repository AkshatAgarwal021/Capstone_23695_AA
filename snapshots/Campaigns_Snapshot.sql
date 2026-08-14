{% snapshot CAMPAIGNS_SNAPSHOT %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='campaign_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        campaign.value:campaign_id::VARCHAR AS campaign_id,

        TRY_TO_TIMESTAMP_NTZ(
            campaign.value:last_modified_date::STRING
        ) AS last_modified_date,

        campaign.value AS raw_campaign_data,

        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__campaigns_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:campaigns_data
    ) AS campaign

),

latest_campaign AS (

    SELECT
        campaign_id,
        last_modified_date,
        raw_campaign_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    campaign_id,
    last_modified_date,
    raw_campaign_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_campaign

{% endsnapshot %}