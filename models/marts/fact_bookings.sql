USE DATABASE ABC_analytics;

USE schema marts;

-- -----------------------------------------------------------------------------
-- FACT: FACT_BOOKINGS (one row per booking)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE marts.fact_bookings AS
SELECT
    booking_id,
    booking_request_date,
    dd.DATE_KEY AS booking_request_date_key,
    booking_status,
    b.driver_id,
    d.dim_driver_id,
    estimated_route_fare,
    CASE
        WHEN d.driver_id IS NULL THEN 1
        ELSE 0
    END AS is_missing_driver
FROM
    staging.vw_stg_bookings b
    LEFT JOIN marts.VW_DIM_DRIVERS d ON d.driver_id = b.driver_id
    LEFT JOIN marts.dim_date dd ON dd.date = DATE(b.booking_request_date);