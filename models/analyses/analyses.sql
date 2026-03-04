USE DATABASE ABC_analytics;

USE SCHEMA analyses;

-- -----------------------------------------------------------------------------
-- Analyses: vw_driver_kpi_summary
-- -----------------------------------------------------------------------------
--Grain: one record per driver

CREATE OR REPLACE VIEW analyses.vw_driver_kpi_summary AS
SELECT
    driver_id,
END AS driver_segment,
SUM(total_offers) AS total_offers,
SUM(offers_accepted) AS offers_accepted,
SUM(OFFERS_CANCELLED) AS OFFERS_CANCELLED,
SUM(total_est_revenue) AS total_est_revenue,
SUM(total_bookings) AS total_bookings,
SUM(completed_trips) AS completed_trips,
SUM(DRIVER_ABORT_COUNT) AS DRIVER_ABORT_COUNT,
round((
        SUM(offers_accepted) / NULLIF(SUM(total_offers), 0)
    ) * 100,2) AS OFFER_ACCEPTANCE_RATE, --as percentage
round((
        SUM(completed_trips) / NULLIF(SUM(total_bookings), 0)
    ) * 100,2) AS BOOKING_COMPLETION_RATE,
round((
        SUM(DRIVER_ABORT_COUNT) / NULLIF(SUM(total_bookings), 0)
    ) * 100,2) AS DRIVER_ABORT_RATE,
round(SUM(total_est_revenue) / NULLIF(SUM(completed_trips), 0) * 100,2) 
    AS AVG_EST_REVENUE_PER_TRIP
FROM
    marts.AGG_DRIVER_ACTIVITY
WHERE
    total_offers <> 0
GROUP BY
    1;

SELECT
    driver_id,
    total_offers,
    OFFER_ACCEPTANCE_RATE,
    BOOKING_COMPLETION_RATE,
    DRIVER_ABORT_RATE
FROM
    analyses.vw_driver_kpi_summary;


-- -----------------------------------------------------------------------------
-- Analyses: vw_weekend_vs_weekday
-- -----------------------------------------------------------------------------
--Grain: one record per driver
CREATE
OR REPLACE VIEW analyses.vw_weekend_vs_weekday AS
SELECT
    CASE
        WHEN WEEKEND_FLAG = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(a.total_offers) AS total_offers,
    SUM(a.offers_accepted) AS offers_accepted,
    SUM(a.total_bookings) AS total_bookings,
    SUM(a.completed_trips) AS completed_trips,
    SUM(a.driver_abort_count) AS driver_abort_count,
    SUM(a.total_revenue) AS total_revenue,
    -- KPI Rates
    SUM(a.offers_accepted) / NULLIF(SUM(a.total_offers), 0) AS offer_acceptance_rate,
    SUM(a.driver_abort_count) / NULLIF(SUM(a.total_bookings), 0) AS driver_abort_rate,
    SUM(a.completed_trips) / NULLIF(SUM(a.total_bookings), 0) AS completion_rate
FROM
    marts.AGG_DRIVER_ACTIVITY a
    JOIN marts.dim_date d ON a.activity_date = d.date_key
GROUP BY
    1;

---------------------------------------
-- with dim_driver, summary stats view for those ids in dim_driver
CREATE
OR REPLACE VIEW analyses.vw_driver_tenure_engagement AS
SELECT
    d.driver_type,
    a.driver_id,
    SUM(a.total_offers) AS total_offers,
    SUM(a.offers_accepted) AS offers_accepted,
    SUM(a.OFFERS_CANCELLED) AS OFFERS_CANCELLED,
    SUM(a.OFFERS_READ) AS OFFERS_READ,
    SUM(a.TOTAL_ROUTE_DISTANCE) AS TOTAL_ROUTE_DISTANCE,
    SUM(a.total_bookings) AS total_bookings,
    SUM(a.completed_trips) AS completed_trips,
    SUM(a.total_est_revenue) AS total_est_revenue,
    SUM(a.DRIVER_ABORT_COUNT) AS DRIVER_ABORT_COUNT,
    SUM(a.offers_accepted) / NULLIF(SUM(a.total_offers), 0) AS OFFER_ACCEPTANCE_RATE,
    SUM(a.completed_trips) / NULLIF(SUM(a.total_bookings), 0) AS BOOKING_COMPLETION_RATE,
    SUM(a.DRIVER_ABORT_COUNT) / NULLIF(SUM(a.total_bookings), 0) AS DRIVER_ABORT_RATE,
    SUM(a.total_est_revenue) / NULLIF(SUM(a.completed_trips), 0) AS AVG_EST_REVENUE_PER_TRIP
FROM
    marts.AGG_DRIVER_ACTIVITY a
    LEFT JOIN marts.vw_dim_drivers d ON a.driver_id = d.driver_id
GROUP BY
    1,
    2;

