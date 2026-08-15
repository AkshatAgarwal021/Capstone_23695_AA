{{ config(
    materialized='table'
) }}

WITH stores AS (

    SELECT

        store_id,
        store_name,
        standardized_address,
        region,
        store_type,
        opening_date,
        store_size_category

    FROM {{ ref('silver__stores_data') }}

),

final AS (

    SELECT

        /*
           SURROGATE KEY

           Generated from the natural Store ID
           using dbt_utils.
        */

        {{ dbt_utils.generate_surrogate_key([
            'store_id'
        ]) }} AS store_key,


        /*
           NATURAL KEY
        */

        store_id,


        /*
           STORE ATTRIBUTES
        */

        store_name,

        standardized_address AS address,

        region,

        store_type,

        opening_date,

        store_size_category AS size_category

    FROM stores

)

SELECT *

FROM final