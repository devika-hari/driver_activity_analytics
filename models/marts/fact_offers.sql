USE DATABASE ABC_analytics;

USE schema marts;

-- -----------------------------------------------------------------------------
-- FACT: FACT_OFFERS (one row per OFFER)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE marts.fact_offers AS
SELECT
    o.offer_id,
    o.offer_created_date,
    dd.DATE_KEY AS offer_created_date_key,
    o.booking_id,
    o.driver_id,
    d.dim_driver_id,
    o.route_distance,
    o.offer_state,
    o.driver_read,
    CASE
        WHEN d.driver_id IS NULL THEN 1
        ELSE 0
    END AS is_missing_driver,
    CASE
        WHEN b.booking_id IS NULL THEN 1
        ELSE 0
    END AS is_missing_booking
FROM
    staging.vw_stg_offers o
    LEFT JOIN staging.vw_stg_bookings b ON b.booking_id = o.booking_id
    LEFT JOIN marts.VW_DIM_DRIVERS d ON d.driver_id = o.driver_id
    LEFT JOIN marts.dim_date dd ON dd.date = DATE(o.offer_created_date);
