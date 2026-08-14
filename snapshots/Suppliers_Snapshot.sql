{% snapshot SUPPLIERS_SNAPSHOT %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='supplier_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        supplier.value:supplier_id::VARCHAR AS supplier_id,

        TRY_TO_TIMESTAMP_NTZ(
            supplier.value:last_modified_date::STRING
        ) AS last_modified_date,

        supplier.value AS raw_supplier_data,

        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__suppliers_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:suppliers_data
    ) AS supplier

),

latest_supplier AS (

    SELECT
        supplier_id,
        last_modified_date,
        raw_supplier_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY supplier_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    supplier_id,
    last_modified_date,
    raw_supplier_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_supplier

{% endsnapshot %}