USE DATABASE ABC_analytics;

USE schema marts;

-- -----------------------------------------------------------------------------
-- AGG: AGG_DRIVER_ACTIVITY (one row per driver per day)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE marts.AGG_DRIVER_ACTIVITY AS 
WITH combined_ids AS (
    SELECT
        driver_id
    FROM marts.vw_dim_drivers
    WHERE 
        registration_date_key <= 20210614

    UNION ALL

    SELECT
        driver_id
    FROM marts.fact_offers
    WHERE
        offer_created_date_key >= 20210601
        AND offer_created_date_key <= 20210614
    UNION ALL

    SELECT
        driver_id
    FROM marts.fact_bookings
    WHERE
        booking_request_date_key >= 20210601
        AND booking_request_date_key <= 20210614
        AND driver_id IS NOT NULL
),
deduped AS(
    SELECT
        driver_id
    FROM combined_ids
    GROUP BY driver_id
),

date_spine AS (
    SELECT
        date_key
    FROM  marts.dim_date
    WHERE
        date_key >= 20210601
        AND date_key <= 20210614
),

driver_date_matrix AS (
    SELECT
        d.date_key AS activity_date_key,
        u.driver_id
    FROM
        date_spine d
        CROSS JOIN deduped u
),

offers_daily AS ( --KPIs from Fact_offers
    SELECT
        driver_id,
        offer_created_date_key AS activity_date_key,
        SUM(route_distance) AS route_distance,
        COUNT_IF(is_missing_driver = 1) AS missing_driver_offers_records,
        COUNT(OFFER_ID) AS total_offers,
        SUM(
            CASE
                WHEN offer_state = 'ACCEPTED' THEN 1
                ELSE 0
            END
        ) AS offers_accepted,
        SUM(
            CASE
                WHEN offer_state = 'CANCELED' THEN 1
                ELSE 0
            END
        ) AS offers_cancelled,
        SUM(
            CASE
                WHEN driver_read = TRUE THEN 1
                ELSE 0
            END
        ) AS offers_read
    FROM
        marts.fact_offers
    WHERE
        offer_created_date_key >= 20210601
        AND offer_created_date_key <= 20210614
    GROUP BY
        1,
        2
) --KPIS FROM FACT_BOOKINGS
,
bookings_daily AS (
    SELECT
        driver_id,
        booking_request_date_key AS activity_date_key,
        COUNT_IF(is_missing_driver = 1) AS missing_driver_bookings_records,
        COUNT(booking_id) AS total_bookings,
        COUNT_IF(booking_status = 'SUCCESS') AS completed_trips,
        COUNT_IF(booking_status = 'DRIVER_ABORT') AS driver_abort_count,
        COUNT_IF(booking_status = 'NO_DRIVER_FOUND') AS no_driver_found_count,
        SUM(
            CASE
                WHEN booking_status = 'SUCCESS' THEN estimated_route_fare
                ELSE 0
            END
        ) AS total_est_revenue
    FROM
        marts.fact_bookings
    WHERE
        booking_request_date_key >= 20210601
        AND booking_request_date_key <= 20210614
    GROUP BY
        1,
        2
)
SELECT
    m.driver_id,
    m.activity_date_key AS activity_date_key,
    TRY_TO_DATE(m.activity_date_key :: TEXT, 'YYYYMMDD') AS activity_date,
    -- Absolute Numbers (Offers)
    COALESCE(o.route_distance, 0) AS total_route_distance,
    COALESCE(o.total_offers, 0) AS total_offers,
    COALESCE(o.offers_accepted, 0) AS offers_accepted,
    COALESCE(o.offers_cancelled, 0) AS offers_cancelled,
    COALESCE(o.offers_read, 0) AS offers_read,
    -- Absolute Numbers (Bookings)
    COALESCE(b.total_bookings, 0) AS total_bookings,
    COALESCE(b.completed_trips, 0) AS completed_trips,
    COALESCE(b.driver_abort_count, 0) AS driver_abort_count,
    COALESCE(b.no_driver_found_count, 0) AS no_driver_found_count,
    COALESCE(b.total_est_revenue, 0) AS total_est_revenue,
    COALESCE(o.missing_driver_offers_records, 0) AS missing_driver_offers_records,
    COALESCE(b.missing_driver_bookings_records, 0) AS missing_driver_bookings_records,
    -- KPI Ratios (Safe Division)
    CASE
        WHEN COALESCE(o.total_offers, 0) > 0 THEN round(o.offers_accepted / o.total_offers, 2)
        ELSE 0
    END AS offer_acceptance_rate,
    CASE
        WHEN COALESCE(b.total_bookings, 0) > 0 THEN round(b.completed_trips / b.total_bookings, 2)
        ELSE 0
    END AS booking_completion_rate,
    CASE
        WHEN COALESCE(b.total_bookings, 0) > 0 THEN round(b.driver_abort_count / b.total_bookings, 2)
        ELSE 0
    END AS driver_abort_rate,
    CASE
        WHEN COALESCE(b.completed_trips, 0) > 0 THEN round(b.total_est_revenue / b.completed_trips, 2)
        ELSE 0
    END AS avg_est_revenue_per_trip
FROM
    driver_date_matrix m
    LEFT JOIN offers_daily o ON m.driver_id = o.driver_id
    AND m.activity_date_key = o.activity_date_key
    LEFT JOIN bookings_daily b ON m.driver_id = b.driver_id
    AND m.activity_date_key = b.activity_date_key;