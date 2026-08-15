{{ config(
    materialized='table'
) }}

WITH date_spine AS (

    /*
       COMPLETE DATE SPINE

       Start: 2024-04-01
       End:   2024-09-27

       dbt_utils.date_spine uses an exclusive end date,
       therefore 2024-09-28 is supplied as the end date.
    */

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-04-01' as date)",
        end_date="cast('2024-09-28' as date)"
    ) }}

),

date_attributes AS (

    SELECT

        /*
           DATE KEY

           YYYYMMDD format.

           Example:
           2024-04-01 -> 20240401
        */

        TO_NUMBER(
            TO_CHAR(
                DATE_DAY,
                'YYYYMMDD'
            )
        ) AS date_key,


        /*
           FULL DATE
        */

        DATE_DAY AS full_date,


        /*
           YEAR
        */

        YEAR(DATE_DAY) AS year,


        /*
           QUARTER
        */

        QUARTER(DATE_DAY) AS quarter,


        /*
           MONTH

           Numeric month: 1-12
        */

        MONTH(DATE_DAY) AS month,


        /*
           WEEK
        */

        WEEK(DATE_DAY) AS week,


        /*
           DAY OF WEEK

           Snowflake:
           0/1/... depending on session settings.
           We also provide a readable day name below.
        */

        DAYOFWEEK(DATE_DAY) AS day_of_week,


        /*
           DAY NAME

           Useful for reporting.
        */

        DAYNAME(DATE_DAY) AS day_name,


        /*
           US HOLIDAY FLAG

           Holidays occurring inside the assignment's
           date window in 2024:

           Memorial Day       - 2024-05-27
           Juneteenth         - 2024-06-19
           Independence Day   - 2024-07-04
           Labor Day          - 2024-09-02
        */

        CASE

            WHEN DATE_DAY IN (
                DATE '2024-05-27',
                DATE '2024-06-19',
                DATE '2024-07-04',
                DATE '2024-09-02'
            )

            THEN TRUE

            ELSE FALSE

        END AS holiday_flag,


        /*
           SEASON

           Northern hemisphere seasons.
        */

        CASE

            WHEN MONTH(DATE_DAY) IN (12, 1, 2)
                THEN 'Winter'

            WHEN MONTH(DATE_DAY) IN (3, 4, 5)
                THEN 'Spring'

            WHEN MONTH(DATE_DAY) IN (6, 7, 8)
                THEN 'Summer'

            WHEN MONTH(DATE_DAY) IN (9, 10, 11)
                THEN 'Fall'

        END AS season

    FROM date_spine

)

SELECT

    date_key,
    full_date,
    year,
    quarter,
    month,
    week,
    day_of_week,
    day_name,
    holiday_flag,
    season

FROM date_attributes