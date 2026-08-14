{% snapshot PRODUCTS_SNAPSHOT %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        product.value:product_id::VARCHAR AS product_id,

        TRY_TO_TIMESTAMP_NTZ(
            product.value:last_modified_date::STRING
        ) AS last_modified_date,

        product.value AS raw_product_data,

        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__products_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:products_data
    ) AS product

),

latest_product AS (

    SELECT
        product_id,
        last_modified_date,
        raw_product_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    product_id,
    last_modified_date,
    raw_product_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_product

{% endsnapshot %}