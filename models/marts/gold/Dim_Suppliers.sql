{{ config(
    materialized='table'
) }}

WITH suppliers AS (

    SELECT

        supplier_id,
        supplier_name,
        contact_name,
        email,
        phone,
        standardized_address,
        payment_terms,
        supplier_type

    FROM {{ ref('silver__suppliers_data') }}

),

final AS (

    SELECT

        /*
           SURROGATE KEY

           Generated from the natural supplier_id.
        */

        {{ dbt_utils.generate_surrogate_key([
            'supplier_id'
        ]) }} AS supplier_key,


        /*
           NATURAL KEY
        */

        supplier_id,


        /*
           SUPPLIER NAME
        */

        supplier_name,


        /*
           CONTACT INFORMATION

           Combine the cleaned contact attributes
           into one reporting-friendly field.
        */

        CONCAT_WS(
            ' | ',

            NULLIF(
                TRIM(contact_name),
                ''
            ),

            NULLIF(
                TRIM(email),
                ''
            ),

            NULLIF(
                TRIM(phone),
                ''
            )

        ) AS contact_information,


        /*
           PAYMENT TERMS
        */

        payment_terms,


        /*
           SUPPLIER TYPE
        */

        supplier_type

    FROM suppliers

)

SELECT *

FROM final