USE DATABASE ABC_analytics;

USE schema marts;

-- -----------------------------------------------------------------------------
-- SP: Stored Procedure to insert into DIM_DRIVERS(SCD2) FROM STG_DRIVERS
-- -----------------------------------------------------------------------------
CREATE
OR REPLACE PROCEDURE ABC_ANALYTICS.MARTS.SP_LOAD_DIM_DRIVERS() RETURNS STRING LANGUAGE SQL AS $ $ BEGIN --update current active record - close it
UPDATE
    marts.dim_drivers target
SET
    end_eff_ts = dateadd('second', -1, source.raw_load_tz),
    is_active = FALSE,
    last_modified_ts = CURRENT_TIMESTAMP()
FROM
    staging.VW_STG_DRIVERS source
WHERE
    target.driver_id = source.driver_id
    AND target.is_active = TRUE
    AND source.IS_PROCESSED = FALSE
    AND (
        target.driver_rating != source.driver_rating
        OR target.country_code != source.country_code
        OR target.receive_marketing != source.receive_marketing
    );

--insert latest record
INSERT INTO
    marts.dim_drivers(
        driver_id,
        registration_date,
        driver_rating,
        driver_rating_count,
        country_name,
        country_code,
        start_eff_ts,
        end_eff_ts,
        is_active,
        last_modified_ts
    )
SELECT
    source.driver_id,
    source.registration_date,
    source.driver_rating,
    source.driver_rating_count,
    source.country_name,
    source.country_code,
    source.raw_load_tz AS start_eff_ts,
    '9999-12-31' :: TIMESTAMP_TZ AS end_eff_ts,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS last_modified_ts
FROM
    staging.vw_stg_drivers source
WHERE
    source.is_processed = FALSE
    AND (
        NOT EXISTS (
            SELECT
                1
            FROM
                ABC_ANALYTICS.MARTS.DIM_DRIVERS target
            WHERE
                target.DRIVER_ID = source.DRIVER_ID
                AND target.is_active = TRUE
        )
    );


--update is_processed flag
UPDATE
    ABC_ANALYTICS.STAGING.STG_DRIVERS
SET
    IS_PROCESSED = TRUE
WHERE
    IS_PROCESSED = FALSE;

RETURN 'DIM_DRIVERS load complete.';

END;

$ $;

call ABC_ANALYTICS.MARTS.SP_LOAD_DIM_DRIVERS();