{% snapshot EMPLOYEES_SNAPSHOT %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        employee.value:employee_id::VARCHAR AS employee_id,

        TRY_TO_TIMESTAMP_NTZ(
            employee.value:last_modified_date::STRING
        ) AS last_modified_date,

        employee.value AS raw_employee_data,

        b.SOURCE_FILE,
        b.LOADED_AT,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__employees_data') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:employees_data
    ) AS employee

),

latest_employee AS (

    SELECT
        employee_id,
        last_modified_date,
        raw_employee_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY employee_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    employee_id,
    last_modified_date,
    raw_employee_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_employee

{% endsnapshot %}