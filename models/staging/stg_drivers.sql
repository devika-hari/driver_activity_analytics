USE DATABASE ABC_analytics;

USE schema staging;

-- -----------------------------------------------------------------------------
-- SP: STORED PROCEDURE to insert into STG_DRIVERS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ABC_ANALYTICS.STAGING.SP_LOAD_STG_DRIVERS() RETURNS STRING LANGUAGE SQL AS $ $ BEGIN
INSERT INTO
    staging.stg_drivers (
        driver_id,
        registration_date,
        driver_rating,
        driver_rating_count,
        receive_marketing,
        country_raw,
        country_name,
        country_code,
        is_valid,
        error_reason,
        is_processed,
        raw_load_tz
    ) WITH deduped AS (
        -- keep latest record per driver since this is a snapshot table
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY
                    load_tz DESC --last arrived as snapshot date isnt available
            ) AS rn
        FROM
            raw.raw_drivers
        WHERE
            IS_PROCESSED = FALSE QUALIFY rn = 1
    ),
    country_mapped AS (
        SELECT
            d.id,
            d.country,
            d.load_tz,
            NULLIF(d.rating_count, 'null') AS org_rating_count,
            -- keeping raw for datatype-error check
            NULLIF(d.rating, 'null') AS org_rating,
            -- keeping raw for error check
            NULLIF(d.date_registration, 'null') AS org_date_registration,
            -- keeping raw for error check
            TRY_TO_TIMESTAMP_TZ(d.date_registration) AS registration_date,
            TRY_TO_DECIMAL(d.rating, 38, 2) AS rating,
            TRY_TO_NUMERIC(d.rating_count) AS rating_count,
            UPPER(TRIM(d.receive_marketing)) AS receive_marketing,
            cm.country_name,
            cm.country_code
        FROM
            deduped d
            LEFT JOIN raw.country_mapping cm ON UPPER(TRIM(d.country)) = UPPER(TRIM(cm.raw_value))
    ),
    dq_checks AS (
        SELECT
            *,
            ARRAY_CONSTRUCT_COMPACT(
                -- build error reason
                CASE
                    WHEN id IS NULL
                    OR LENGTH(TRIM(id)) = 0 THEN 'NULL/EMPTY ID | '
                END,
                CASE
                    WHEN registration_date IS NULL
                    OR registration_date > CURRENT_TIMESTAMP() --future date not allowed
                    OR registration_date < '2009-06-01' :: TIMESTAMP_TZ --Freenow founding date variable
                    THEN 'INVALID DATE_REGISTRATION'
                END,
                CASE
                    WHEN org_date_registration IS NOT NULL
                    AND registration_date IS NULL --datatype conversion error
                    THEN 'DATE_REGISTRATION FORMAT ERROR: ' || org_date_registration
                END,
                CASE
                    WHEN org_rating IS NOT NULL
                    AND rating IS NULL THEN 'RATING FORMAT ERROR: ' || org_rating
                END,
                CASE
                    WHEN org_rating_count IS NOT NULL
                    AND rating_count IS NULL THEN 'COUNT FORMAT ERROR: ' || org_rating_count
                END,
                CASE
                    WHEN rating < 1.0
                    OR rating > 5.0 THEN 'DRIVER_RATING OUT OF RANGE '
                END,
                CASE
                    WHEN rating_count < 0 THEN 'NEGATIVE RATING_COUNT '
                END,
                CASE
                    WHEN (
                        rating_count = 0
                        OR rating_count IS NULL
                    )
                    AND rating >= 1 THEN 'RATING EXISTS BUT RATING_COUNT IS 0/NULL  '
                END,
                CASE
                    WHEN (
                        rating = 0
                        OR rating IS NULL
                    )
                    AND rating_count > 0 THEN 'RATING_COUNT EXISTS BUT RATING IS 0/NULL  '
                END,
                CASE
                    WHEN receive_marketing NOT IN ('TRUE', 'FALSE', '1', '0', 'YES', 'NO') THEN 'INVALID RECEIVE_MARKETING '
                END,
                CASE
                    WHEN country IS NULL
                    OR LENGTH(TRIM(country)) = 0 THEN 'NULL COUNTRY '
                END,
                CASE
                    WHEN country_code IS NULL THEN 'UNMAPPED COUNTRY: ' || NVL(country, 'NULL')
                END
            ) AS error_reason
        FROM
            country_mapped
    )
SELECT
    id AS driver_id,
    registration_date AS registration_date,
    rating AS driver_rating,
    rating_count AS driver_rating_count,
    CASE
        WHEN receive_marketing IN ('TRUE', '1', 'YES') THEN TRUE
        WHEN receive_marketing IN ('FALSE', '0', 'NO') THEN FALSE
        ELSE NULL
    END AS receive_marketing,
    country AS country_raw,
    -- preserve original
    country_name,
    country_code,
    ARRAY_SIZE(error_reason) = 0 AS is_valid,
    ARRAY_TO_STRING(error_reason, ' | ') AS error_reason,
    FALSE AS is_processed,
    load_tz AS raw_load_tz
FROM
    dq_checks;

UPDATE
    raw.raw_drivers
SET
    IS_PROCESSED = TRUE;

RETURN 'STG_DRIVERS load complete.';

END;

$ $;

call ABC_ANALYTICS.STAGING.SP_LOAD_STG_DRIVERS();

