{{ config(
    materialized='table'
) }}

WITH campaigns AS (

    SELECT

        campaign_key,
        campaign_id,
        budget,
        start_date,
        end_date

    FROM {{ ref('Dim_MarketingCampaign') }}

),

dates AS (

    SELECT

        date_key,
        full_date

    FROM {{ ref('Dim_date') }}

),

/*
   ============================================================
   1. SALES BASE
   ============================================================

   FACT_Sales is represented here using the Orders Silver
   data because there is no separate FACT_Sales Gold model.

   Required attributes:

       CampaignKey
       DateKey
       CustomerKey
       TotalSalesAmount
*/

sales_base AS (

    SELECT

        o.order_id,


        /*
           CAMPAIGN KEY

           Generated from the natural Campaign ID.
        */

        {{ dbt_utils.generate_surrogate_key([
            'o.campaign_id'
        ]) }} AS campaign_key,


        /*
           DATE KEY

           DateKey is generated in YYYYMMDD format.
        */

        TO_NUMBER(
            TO_CHAR(
                o.order_date,
                'YYYYMMDD'
            )
        ) AS date_key,


        /*
           CUSTOMER KEY

           Generated from the natural Customer ID.
        */

        {{ dbt_utils.generate_surrogate_key([
            'o.customer_id'
        ]) }} AS customer_key,


        /*
           NATURAL KEYS
        */

        o.customer_id,
        o.campaign_id,
        o.order_date,


        /*
           TOTAL SALES AMOUNT
        */

        COALESCE(
            o.total_amount,
            0.00
        ) AS total_sales_amount

    FROM {{ ref('silver__orders_data') }} o

    WHERE o.order_id IS NOT NULL

),

/*
   ============================================================
   2. CUSTOMER PURCHASE HISTORY
   ============================================================

   Determine the customer's first-ever purchase date.

   This is required to identify first and repeat purchases.
*/

customer_purchase_history AS (

    SELECT

        customer_id,

        MIN(order_date) AS first_purchase_date

    FROM sales_base

    WHERE customer_id IS NOT NULL

    GROUP BY

        customer_id

),

/*
   ============================================================
   3. SALES WITH PURCHASE FLAGS
   ============================================================

   is_first_purchase:

       TRUE when the order occurred on the customer's
       first-ever purchase date.

   is_repeat_purchase:

       TRUE when the customer has purchased previously.

   A customer is not counted multiple times at the
   campaign level because the later aggregation uses
   DISTINCT customer_key.
*/

fact_sales AS (

    SELECT

        s.order_id,

        s.campaign_key,
        s.date_key,
        s.customer_key,

        s.customer_id,
        s.campaign_id,
        s.order_date,

        s.total_sales_amount,


        /*
           FIRST PURCHASE
        */

        CASE

            WHEN s.order_date =
                 h.first_purchase_date

                THEN TRUE

            ELSE FALSE

        END AS is_first_purchase,


        /*
           REPEAT PURCHASE
        */

        CASE

            WHEN s.order_date >
                 h.first_purchase_date

                THEN TRUE

            ELSE FALSE

        END AS is_repeat_purchase

    FROM sales_base s

    LEFT JOIN customer_purchase_history h

        ON s.customer_id =
           h.customer_id

),

/*
   ============================================================
   4. CAMPAIGN CUSTOMER POPULATION
   ============================================================

   Grain:

       one row per campaign per customer

   A customer is considered campaign-influenced when
   they have Sales associated with that campaign during
   the campaign's active period.
*/

campaign_customers AS (

    SELECT DISTINCT

        c.campaign_key,
        c.campaign_id,

        fs.customer_key,
        fs.customer_id,

        c.start_date,
        c.end_date

    FROM campaigns c

    INNER JOIN fact_sales fs

        ON fs.campaign_key =
           c.campaign_key

        AND fs.order_date BETWEEN
            c.start_date
            AND c.end_date

    WHERE fs.customer_key IS NOT NULL

),

/*
   ============================================================
   5. CUSTOMER-LEVEL CAMPAIGN PURCHASE STATUS
   ============================================================

   Grain:

       one row per campaign per customer

   This prevents multiple orders from the same customer
   from inflating the customer metrics.
*/

campaign_customer_status AS (

    SELECT

        cc.campaign_key,
        cc.customer_key,


        /*
           FIRST PURCHASE

           Customer made their first-ever purchase
           during the campaign period.
        */

        MAX(
            CASE

                WHEN fs.is_first_purchase = TRUE

                    THEN 1

                ELSE 0

            END
        ) AS is_first_purchase,


        /*
           REPEAT PURCHASE

           Customer made a purchase after their
           first-ever purchase.

           MAX() ensures that the customer is counted
           only once even if they made multiple
           repeat purchases.
        */

        MAX(
            CASE

                WHEN fs.is_repeat_purchase = TRUE

                    THEN 1

                ELSE 0

            END
        ) AS is_repeat_purchase

    FROM campaign_customers cc

    INNER JOIN fact_sales fs

        ON fs.campaign_key =
           cc.campaign_key

        AND fs.customer_key =
            cc.customer_key

        AND fs.order_date BETWEEN
            cc.start_date
            AND cc.end_date

    GROUP BY

        cc.campaign_key,
        cc.customer_key

),

/*
   ============================================================
   6. CAMPAIGN CUSTOMER METRICS
   ============================================================

   These metrics are calculated at campaign/customer level.

   TOTAL CAMPAIGN CUSTOMERS:

       All distinct customers influenced by the campaign.

   FIRST PURCHASE CUSTOMERS:

       Customers whose first-ever purchase occurred
       during the campaign.

   REPEAT PURCHASE CUSTOMERS:

       Customers who made a repeat purchase during
       the campaign.

   The total campaign customer count is used as the
   denominator for Repeat Purchase Rate because the PS
   describes the metric as the share of campaign-
   influenced customers who made a repeat purchase.
*/

campaign_customer_metrics AS (

    SELECT

        campaign_key,


        /*
           TOTAL CAMPAIGN-INFLUENCED CUSTOMERS
        */

        COUNT(
            DISTINCT customer_key
        ) AS total_campaign_customers,


        /*
           FIRST-PURCHASE CUSTOMERS
        */

        COUNT(
            DISTINCT CASE

                WHEN is_first_purchase = 1

                    THEN customer_key

                ELSE NULL

            END
        ) AS first_purchase_customers,


        /*
           REPEAT-PURCHASE CUSTOMERS
        */

        COUNT(
            DISTINCT CASE

                WHEN is_repeat_purchase = 1

                    THEN customer_key

                ELSE NULL

            END
        ) AS repeat_purchase_customers

    FROM campaign_customer_status

    GROUP BY

        campaign_key

),

/*
   ============================================================
   7. CAMPAIGN / DATE GRAIN
   ============================================================

   Required FACT_MarketingPerformance grain:

       one row per campaign per date
*/

campaign_dates AS (

    SELECT

        c.campaign_key,
        c.campaign_id,

        c.budget,

        c.start_date,
        c.end_date,

        d.date_key,
        d.full_date

    FROM campaigns c

    INNER JOIN dates d

        ON d.full_date BETWEEN
            c.start_date
            AND c.end_date

),

/*
   ============================================================
   8. TOTAL SALES INFLUENCED BY CAMPAIGN
   ============================================================

   PS LOGIC:

       SUM(FACT_Sales.TotalSalesAmount)

   WHERE:

       FACT_Sales.CampaignKey = Campaign.CampaignKey

       AND FACT_Sales.DateKey BETWEEN
           Campaign.StartDate
           AND Campaign.EndDate

   Since the fact grain is campaign + date, the sales
   are aggregated for each campaign/date combination.
*/

sales_influenced AS (

    SELECT

        cd.campaign_key,
        cd.date_key,

        COALESCE(
            SUM(
                fs.total_sales_amount
            ),
            0.00
        ) AS total_sales_influenced

    FROM campaign_dates cd

    LEFT JOIN fact_sales fs

        ON fs.campaign_key =
           cd.campaign_key

        AND fs.date_key =
            cd.date_key

        AND fs.order_date BETWEEN
            cd.start_date
            AND cd.end_date

    GROUP BY

        cd.campaign_key,
        cd.date_key

),

/*
   ============================================================
   9. NEW CUSTOMERS ACQUIRED
   ============================================================

   PS LOGIC:

       COUNT(DISTINCT CustomerKey)

   WHERE:

       s.CampaignKey = Campaign.CampaignKey

       AND s.DateKey BETWEEN
           Campaign.StartDate
           AND Campaign.EndDate

       AND s.CustomerKey NOT IN (

           SELECT prior.CustomerKey
           FROM FACT_Sales prior
           WHERE prior.DateKey < Campaign.StartDate

       )

   NOT EXISTS is used instead of NOT IN to safely handle
   NULL customer values.
*/

new_customers AS (

    SELECT

        cd.campaign_key,
        cd.date_key,

        COUNT(
            DISTINCT CASE

                WHEN fs.customer_key IS NOT NULL

                     AND fs.order_date BETWEEN
                         cd.start_date
                         AND cd.end_date

                     AND NOT EXISTS (

                         SELECT 1

                         FROM fact_sales prior

                         WHERE prior.customer_key =
                               fs.customer_key

                           AND prior.order_date <
                               cd.start_date

                     )

                    THEN fs.customer_key

                ELSE NULL

            END
        ) AS new_customers_acquired

    FROM campaign_dates cd

    LEFT JOIN fact_sales fs

        ON fs.campaign_key =
           cd.campaign_key

        AND fs.date_key =
            cd.date_key

    GROUP BY

        cd.campaign_key,
        cd.date_key

),

/*
   ============================================================
   10. FINAL METRIC ASSEMBLY
   ============================================================
*/

metrics AS (

    SELECT

        cd.campaign_key,
        cd.campaign_id,

        cd.date_key,
        cd.full_date,

        cd.budget,

        cd.start_date,
        cd.end_date,


        /*
           TOTAL SALES INFLUENCED
        */

        COALESCE(
            si.total_sales_influenced,
            0.00
        ) AS total_sales_influenced,


        /*
           NEW CUSTOMERS ACQUIRED
        */

        COALESCE(
            nc.new_customers_acquired,
            0
        ) AS new_customers_acquired,


        /*
           TOTAL CAMPAIGN CUSTOMERS
        */

        COALESCE(
            ccm.total_campaign_customers,
            0
        ) AS total_campaign_customers,


        /*
           FIRST PURCHASE CUSTOMERS
        */

        COALESCE(
            ccm.first_purchase_customers,
            0
        ) AS first_purchase_customers,


        /*
           REPEAT PURCHASE CUSTOMERS
        */

        COALESCE(
            ccm.repeat_purchase_customers,
            0
        ) AS repeat_purchase_customers

    FROM campaign_dates cd

    LEFT JOIN sales_influenced si

        ON cd.campaign_key =
           si.campaign_key

        AND cd.date_key =
            si.date_key

    LEFT JOIN new_customers nc

        ON cd.campaign_key =
           nc.campaign_key

        AND cd.date_key =
            nc.date_key

    LEFT JOIN campaign_customer_metrics ccm

        ON cd.campaign_key =
           ccm.campaign_key

),

/*
   ============================================================
   11. FINAL FACT
   ============================================================

   Grain:

       one row per campaign per date
*/

final AS (

    SELECT

        /*
           SURROGATE KEY

           Campaign + Date defines the fact grain.
        */

        {{ dbt_utils.generate_surrogate_key([
            'campaign_key',
            'date_key'
        ]) }} AS marketing_performance_key,


        /*
           DIMENSION KEYS
        */

        campaign_key,
        date_key,


        /*
           TRACEABILITY
        */

        campaign_id,
        full_date,


        /*
           TOTAL SALES INFLUENCED
        */

        total_sales_influenced,


        /*
           NEW CUSTOMERS ACQUIRED
        */

        new_customers_acquired,


        /*
           REPEAT PURCHASE RATE

           Business definition from the PS:

           Share of campaign-influenced customers
           who made a repeat purchase.

           Formula:

               Repeat Purchase Customers
               --------------------------------
               Total Campaign Customers
               
               * 100

           Because both numerator and denominator
           contain DISTINCT customers, the result
           cannot exceed 100%.
        */

        CASE

            WHEN total_campaign_customers > 0

                THEN
                    100.0
                    * repeat_purchase_customers
                    / NULLIF(
                        total_campaign_customers,
                        0
                    )

            ELSE NULL

        END AS repeat_purchase_rate,


        /*
           TOTAL CAMPAIGN COST
        */

        budget AS total_campaign_cost,


        /*
           ROI

           PS LOGIC:

               ROI =
               CASE
                   WHEN TotalCampaignCost > 0
                   THEN
                       (
                           TotalSalesInfluenced
                           - TotalCampaignCost
                       )
                       / TotalCampaignCost * 100
                   ELSE NULL
               END
        */

        CASE

            WHEN budget > 0

                THEN
                    (
                        total_sales_influenced
                        - budget
                    )
                    / budget * 100

            ELSE NULL

        END AS roi

    FROM metrics

)

/*
   ============================================================
   FINAL OUTPUT
   ============================================================
*/

SELECT

    marketing_performance_key,

    campaign_key,
    date_key,

    campaign_id,
    full_date,

    total_sales_influenced,
    new_customers_acquired,

    repeat_purchase_rate,

    total_campaign_cost,
    roi

FROM final