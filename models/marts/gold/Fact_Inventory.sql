{{ config(
    materialized = 'table'
) }}

WITH inventory AS (

    SELECT

        inventory_key,

        product_id,
        store_id,
        inventory_date,

        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,

        inventory_value,
        stock_turnover_ratio,
        supplier_contribution_percentage,

        supplier_id,

        /*
           INVENTORY SNAPSHOT QUALITY

           These two fields already come from
           Silver Inventory.
        */

        snapshot_gap_flag,
        snapshot_gap_days

    FROM {{ ref('silver__inventory_data') }}

),

products AS (

    SELECT

        product_key,
        product_id

    FROM {{ ref('Dim_Products') }}

),

stores AS (

    SELECT

        store_key,
        store_id

    FROM {{ ref('Dim_Stores') }}

),

suppliers AS (

    SELECT

        supplier_key,
        supplier_id

    FROM {{ ref('Dim_Suppliers') }}

),

dates AS (

    SELECT

        date_key,
        full_date

    FROM {{ ref('Dim_date') }}

),

final AS (

    SELECT

        /*
           INVENTORY KEY

           Grain:
           Product + Store + Date
        */

        i.inventory_key,


        /*
           DIMENSION KEYS
        */

        p.product_key,

        d.date_key,

        st.store_key,

        s.supplier_key,


        /*
           INVENTORY MEASURES
        */

        i.beginning_stock,

        i.purchased_quantity,

        i.sold_quantity,

        i.ending_stock,

        i.inventory_value,

        i.stock_turnover_ratio,

        i.supplier_contribution_percentage,


        /*
           INVENTORY SNAPSHOT QUALITY

           Added from Silver so that the
           reporting layer can identify
           on-time vs delayed snapshots.
        */

        i.snapshot_gap_flag,

        i.snapshot_gap_days

    FROM inventory i


    /*
       PRODUCT DIMENSION

       Inventory.product_id
       ->
       Dim_Products.product_id
    */

    LEFT JOIN products p

        ON i.product_id = p.product_id


    /*
       STORE DIMENSION

       Inventory.store_id
       ->
       Dim_Stores.store_id
    */

    LEFT JOIN stores st

        ON i.store_id = st.store_id


    /*
       SUPPLIER DIMENSION

       Inventory.supplier_id
       ->
       Dim_Suppliers.supplier_id
    */

    LEFT JOIN suppliers s

        ON i.supplier_id = s.supplier_id


    /*
       DATE DIMENSION

       Inventory.inventory_date
       ->
       Dim_date.full_date
    */

    LEFT JOIN dates d

        ON i.inventory_date = d.full_date

)

SELECT

    inventory_key,

    product_key,
    date_key,
    store_key,
    supplier_key,

    beginning_stock,
    purchased_quantity,
    sold_quantity,
    ending_stock,

    inventory_value,
    stock_turnover_ratio,
    supplier_contribution_percentage,

    /*
       Snapshot monitoring fields
    */

    snapshot_gap_flag,
    snapshot_gap_days

FROM final