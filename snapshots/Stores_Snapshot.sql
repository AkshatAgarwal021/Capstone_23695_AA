{% snapshot STORES_SNAPSHOT %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='store_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        store.value:store_id::VARCHAR AS store_id,

        TRY_TO_TIMESTAMP_NTZ(
            store.value:last_modified_date::STRING
        ) AS last_modified_date,

        store.value AS raw_store_data,

        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__stores_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:stores_data
    ) AS store

),

latest_store AS (

    SELECT
        store_id,
        last_modified_date,
        raw_store_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    store_id,
    last_modified_date,
    raw_store_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_store

{% endsnapshot %}