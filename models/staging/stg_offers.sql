USE DATABASE ABC_analytics;
USE schema staging;

- -----------------------------------------------------------------------------
-- SP: STORED PROCEDURE to insert into STG_OFFERS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ABC_ANALYTICS.STAGING.SP_LOAD_STG_OFFERS() RETURNS STRING LANGUAGE SQL AS $ $ BEGIN
INSERT INTO
    staging.stg_offers (
        offer_id,
        OFFER_created_DATE,
        booking_id,
        driver_id,
        route_distance,
        OFFER_state,
        driver_read,
        is_valid,
        error_reason,
        is_processed,
        raw_load_tz,
        is_missing_driver,
        is_missing_booking
    ) WITH deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY
                    load_tz DESC
            ) AS rn
        FROM
            raw.raw_offers
        WHERE
            IS_PROCESSED = FALSE 
            QUALIFY rn = 1
    ),
    dq_checks AS (
        SELECT
            o.id AS offer_id,
            datecreated AS org_datecreated,
            TRY_TO_TIMESTAMP_TZ(o.datecreated) AS OFFER_created_DATE,
            o.bookingid,
            NULLIF(o.driverid, 'null') AS driverid,
            NULLIF(o.routedistance, 'null') AS org_routedistance,
            try_to_decimal(NULLIF(o.routedistance, 'null'), 38, 2) AS route_distance,
            Upper(trim(o.state)) AS offer_state,
            Upper(trim(o.driverread)) AS driver_read,
            load_tz AS raw_load_tz,
            ARRAY_CONSTRUCT_COMPACT(
                CASE
                    WHEN o.id IS NULL
                    OR LENGTH(TRIM(o.id)) = 0 THEN 'INVALID ID'
                END,
                CASE
                    WHEN driverid IS NULL THEN 'Null in driverid'
                END,
                CASE
                    WHEN OFFER_created_DATE IS NULL
                    OR OFFER_created_DATE > CURRENT_TIMESTAMP()
                    OR OFFER_created_DATE < '2009-06-01' :: TIMESTAMP_TZ THEN 'INVALID DATECREATED '
                END,
                CASE
                    WHEN org_datecreated IS NOT NULL
                    AND OFFER_created_DATE IS NULL THEN 'DATE_CREATED FORMAT ERROR: ' || org_datecreated
                END,
                CASE
                    WHEN o.bookingid IS NULL THEN 'NULL BOOKINGID  '
                END,
                CASE
                    WHEN route_distance < 0
                    OR route_distance > 500000 THEN 'INVALID ROUTEDISTANCE'
                END,
                CASE
                    WHEN org_routedistance IS NOT NULL
                    AND route_distance IS NULL THEN 'ROUTE_DISTANCE FORMAT ERROR: ' || org_routedistance
                END,
                CASE
                    WHEN OFFER_state IS NULL
                    OR OFFER_state NOT IN ('ACCEPTED', 'CANCELED') THEN 'INVALID STATE'
                END,
                CASE
                    WHEN driver_read IS NULL
                    OR driver_read NOT IN ('TRUE', 'FALSE', '0', '1') THEN 'INVALID DRIVERREAD '
                END
            ) AS error_reason,
            CASE
                WHEN d.driver_id IS NULL
                AND o.driverid IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_missing_driver,
            CASE
                WHEN b.booking_id IS NULL
                AND o.bookingid IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_missing_booking
        FROM
            deduped o
            LEFT JOIN staging.vw_stg_bookings b ON o.bookingid = b.booking_id
            LEFT JOIN staging.vw_stg_drivers d ON d.driver_id = o.driverid
    )
SELECT
    offer_id,
    offer_created_date,
    bookingid AS booking_id,
    driverid AS driver_id,
    route_distance,
    offer_state,
    CASE
        WHEN driver_read IN ('TRUE', '1', 'YES') THEN TRUE
        WHEN driver_read IN ('FALSE', '0', 'NO') THEN FALSE
        ELSE NULL
    END AS driver_read,
    ARRAY_SIZE(error_reason) = 0 AS is_valid,
    ARRAY_TO_STRING(error_reason, ' | ') AS error_reason,
    FALSE AS is_processed,
    raw_load_tz,
    is_missing_driver,
    is_missing_booking
FROM
    dq_checks;


UPDATE
    raw.raw_OFFERS
SET
    IS_PROCESSED = TRUE;

RETURN 'STG_OFFERS load complete.';

END;

$ $;

call ABC_ANALYTICS.STAGING.SP_LOAD_STG_OFFERS();

