USE DATABASE ABC_analytics;

USE schema staging;

-- -----------------------------------------------------------------------------
-- SP: STORED PROCEDURE to insert into STG_BOOKINGS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ABC_ANALYTICS.STAGING.SP_LOAD_STG_BOOKINGS() RETURNS STRING LANGUAGE SQL AS $ $ BEGIN
INSERT INTO
    staging.stg_bookings (
        booking_id,
        booking_request_date,
        booking_status,
        driver_id,
        estimated_route_fare,
        is_valid,
        error_reason,
        is_processed,
        raw_load_tz,
        is_missing_driver
    ) WITH deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY
                    load_tz DESC
            ) AS rn
        FROM
            raw.raw_bookings
        WHERE
            IS_PROCESSED = FALSE QUALIFY rn = 1
    ),
    converted AS (
        SELECT
            id,
            request_date AS org_request_date,
            TRY_TO_TIMESTAMP_TZ(request_date) AS request_date,
            UPPER(TRIM(STATUS)) AS STATUS,
            NULLIF(id_driver, 'null') AS id_driver,
            NULLIF(estimated_route_fare, 'null') AS org_estimated_route_fare,
            TRY_TO_DECIMAL(estimated_route_fare, 38, 2) AS estimated_route_fare,
            load_tz
        FROM
            deduped
    ),
    dq_checks AS (
        SELECT
            b.id,
            b.request_date,
            b.status,
            b.id_driver,
            b.estimated_route_fare,
            b.load_tz,
            ARRAY_CONSTRUCT_COMPACT(
                CASE
                    WHEN b.id IS NULL
                    OR LENGTH(TRIM(b.id)) = 0 THEN 'NULL/EMPTY ID'
                END,
                CASE
                    WHEN b.request_date IS NULL
                    OR b.request_date > CURRENT_TIMESTAMP()
                    OR b.request_date < '2009-01-01' :: TIMESTAMP_TZ THEN 'INVALID REQUEST_DATE'
                END,
                CASE
                    WHEN org_request_date IS NOT NULL
                    AND request_date IS NULL THEN 'REQUEST_DATE FORMAT ERROR: |' || org_request_date
                END,
                CASE
                    WHEN b.status IS NULL
                    OR b.status NOT IN (
                        'SERVER_ABORT',
                        'SUCCESS',
                        'DRIVER_ABORT',
                        'NO_DRIVER_FOUND',
                        'PASSENGER_ABORT'
                    ) THEN 'INVALID STATUS: |' || COALESCE(b.status, 'NULL')
                END,
                CASE
                    WHEN b.estimated_route_fare < 0 THEN 'INVALID ESTIMATED_ROUTE_FARE'
                END,
                CASE
                    WHEN org_estimated_route_fare IS NOT NULL
                    AND estimated_route_fare IS NULL THEN 'ESTIMATED_ROUTE_FARE FORMAT ERROR: |' || org_estimated_route_fare
                END
            ) AS error_reason,
            CASE
                WHEN d.driver_id IS NULL
                AND b.id_driver IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_missing_driver --RI check fail indicator
        FROM
            converted b
            LEFT JOIN staging.vw_stg_drivers d ON b.id_driver = d.driver_id
    )
SELECT
    id,
    request_date,
    STATUS,
    id_driver,
    estimated_route_fare AS estimated_route_fare,
    ARRAY_SIZE(error_reason) = 0 AS is_valid,
    ARRAY_TO_STRING(error_reason, ' | ') AS error_reason,
    FALSE AS is_processed,
    load_tz AS row_load_tz,
    is_missing_driver
FROM
    dq_checks;

UPDATE
    raw.raw_BOOKINGS
SET
    IS_PROCESSED = TRUE;

RETURN 'STG_BOOKINGS load complete.';

END;

$ $;

call ABC_ANALYTICS.STAGING.SP_LOAD_STG_BOOKINGS();