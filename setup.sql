-- ---------------------------------------------------------------------------
-- Filename: setup.sql 
-- Description: Create role, warehouse, database, schemas, tables and views in Snowflake
-- ----------------------------------------------------------------------------

-- for initial setup
USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------
-- Create role, warehouse
-- -----------------------------------------------------
-- Create dedicated role for analytics
CREATE OR REPLACE ROLE driver_analytics_role;

-- Grant role to user
GRANT ROLE driver_analytics_role TO USER USER_NAME;

-- Create warehouse for transformations
CREATE OR REPLACE WAREHOUSE ABC_WH
WITH 
    WAREHOUSE_SIZE = 'X-SMALL' -- Start small and increase size on requirement
    AUTO_SUSPEND = 300        -- Suspend after 5 mins inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Allow driver_analytics_role to use warehouse
GRANT USAGE ON WAREHOUSE ABC_WH TO ROLE driver_analytics_role;

-- -----------------------------------------------------
-- Create database
-- -----------------------------------------------------
CREATE OR REPLACE DATABASE ABC_ANALYTICS
COMMENT = 'Contains data models to analyze driver engagement and performance';

-- Grant database usage
GRANT USAGE ON DATABASE ABC_ANALYTICS TO ROLE driver_analytics_role;

-- -----------------------------------------------------
-- Create schemas
-- -----------------------------------------------------
USE DATABASE ABC_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS raw;       -- Raw ingested data
CREATE SCHEMA IF NOT EXISTS staging;   -- Cleaned (Data Quality checked) data
CREATE SCHEMA IF NOT EXISTS marts;     -- Aggregated business models
CREATE SCHEMA IF NOT EXISTS utils;     -- Helpers - stages
CREATE SCHEMA IF NOT EXISTS analyses;  -- Analysis layer

-- Grant schema privileges
GRANT ALL PRIVILEGES ON SCHEMA ABC_ANALYTICS.raw TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON SCHEMA ABC_ANALYTICS.staging TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON SCHEMA ABC_ANALYTICS.marts TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON SCHEMA ABC_ANALYTICS.utils TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON SCHEMA ABC_ANALYTICS.analyses TO ROLE driver_analytics_role;

-- Future grants
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ABC_ANALYTICS.raw TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ABC_ANALYTICS.staging TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ABC_ANALYTICS.marts TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ABC_ANALYTICS.utils TO ROLE driver_analytics_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ABC_ANALYTICS.analyses TO ROLE driver_analytics_role;

-- Switch to driver_analytics_role for development

USE ROLE driver_analytics_role;
USE WAREHOUSE ABC_WH;
USE DATABASE ABC_ANALYTICS;

-- -----------------------------------------------------------------------------
-- Raw: Drivers (snapshot of last info per driver)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ABC_ANALYTICS.RAW.RAW_DRIVERS (
    id VARCHAR(256),
    country VARCHAR(100),
    rating VARCHAR(50),
    rating_count VARCHAR(50),
    date_registration VARCHAR(100),
    receive_marketing VARCHAR(20),
    is_processed BOOLEAN DEFAULT FALSE,
    load_tz TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Raw: Bookings (one row per booking)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ABC_ANALYTICS.RAW.RAW_BOOKINGS (
    id VARCHAR(256),
    request_date VARCHAR(100),
    STATUS VARCHAR(50),
    id_driver VARCHAR(256),
    estimated_route_fare VARCHAR(50),
    is_processed BOOLEAN DEFAULT FALSE,
    load_tz TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Raw: Offers (one row per offer sent to drivers)
-- -----------------------------------------------------------------------------
CREATE
OR REPLACE TABLE ABC_ANALYTICS.RAW.RAW_OFFERS (
    id VARCHAR(256),
    datecreated VARCHAR(100),
    bookingid VARCHAR(256),
    driverid VARCHAR(256),
    routedistance VARCHAR(50),
    state VARCHAR(50),
    driverread VARCHAR(20),
    is_processed BOOLEAN DEFAULT FALSE,
    load_tz TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- To create internal stages
USE schema utils;

--file format for loading source csv files
CREATE OR REPLACE FILE FORMAT csv_gz_format TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 COMPRESSION = AUTO;

-- Internal stage creation
CREATE OR REPLACE STAGE int_offers_stage FILE_FORMAT = csv_gz_format;

--In CLI, moved local file to stage
--snowsql
--PUT file:///Users/srinathsoman/Downloads/AE_Case_Study/ABC_offers.csv.gz @int_offers_stage;

-- Raw tables are loaded
COPY INTO raw.raw_offers (
    id,
    datecreated,
    bookingid,
    driverid,
    routedistance,
    state,
    driverread
)
FROM
    @int_offers_stage FILE_FORMAT = csv_gz_format;

-- -----------------------------------------------------------------------------
-- Mapping: Country_mapping (Mapping table for standardising country information)
-- -----------------------------------------------------------------------------
CREATE TABLE raw.country_mapping (
    raw_value VARCHAR, -- exactly as it appears in source data
    country_name VARCHAR, -- standardized full name
    country_code VARCHAR(2) -- 2-letter code
);

INSERT INTO
    raw.country_mapping (raw_value, country_name, country_code)
VALUES
    ('AUSTRIA', 'Austria', 'AT'),
    ('AT', 'Austria', 'AT'),
    ('ÖSTERREICH', 'Austria', 'AT'),
    ('FRANCE', 'France', 'FR'),
    ('FR', 'France', 'FR');

USE schema staging;
-- -----------------------------------------------------------------------------
-- STG: STG_DRIVERS 
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ABC_ANALYTICS.STAGING.STG_DRIVERS (
    DRIVER_ID VARCHAR(64),
    COUNTRY_RAW VARCHAR(100),
    COUNTRY_NAME VARCHAR(100), 
    COUNTRY_CODE VARCHAR(2),
    DRIVER_RATING NUMBER(38, 2),
    DRIVER_RATING_COUNT NUMBER(38, 0),
    registration_date TIMESTAMP_TZ,
    RECEIVE_MARKETING BOOLEAN,
    IS_VALID BOOLEAN,
    ERROR_REASON VARCHAR(500),
    IS_PROCESSED BOOLEAN DEFAULT FALSE,
    RAW_LOAD_TZ TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP --batch load identifier
);

-- -----------------------------------------------------------------------------
-- VIEW: VW_STG_DRIVERS (View with valid records)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ABC_ANALYTICS.STAGING.VW_STG_DRIVERS AS
SELECT
    DRIVER_ID,
    COUNTRY_NAME,
    COUNTRY_CODE,
    DRIVER_RATING,
    DRIVER_RATING_COUNT,
    registration_date,
    RECEIVE_MARKETING,
    IS_PROCESSED,
    RAW_LOAD_TZ
FROM
    ABC_ANALYTICS.STAGING.STG_DRIVERS
WHERE
    IS_VALID = TRUE;

-- -----------------------------------------------------------------------------
-- STG: STG_Bookings 
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ABC_ANALYTICS.STAGING.STG_Bookings (
    BOOKING_id VARCHAR(64),
    BOOKING_Request_date TIMESTAMP_TZ,
    BOOKING_status VARCHAR(50),
    driver_ID VARCHAR(64),
    estimated_route_fare NUMBER(38, 2),
    IS_VALID BOOLEAN,
    ERROR_REASON VARCHAR(500),
    IS_PROCESSED BOOLEAN DEFAULT FALSE,
    RAW_LOAD_TZ TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,
    is_missing_driver BOOLEAN 
);

-- -----------------------------------------------------------------------------
-- VIEW: VW_STG_BOOKINGS (View with valid records)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ABC_ANALYTICS.STAGING.VW_STG_BOOKINGS AS
SELECT
    BOOKING_id,
    BOOKING_Request_date,
    BOOKING_status,
    driver_ID,
    estimated_route_fare,
    IS_PROCESSED,
    RAW_LOAD_TZ,
    is_missing_driver
FROM
    ABC_ANALYTICS.STAGING.STG_BOOKINGS
WHERE
    IS_VALID = TRUE;

-- -----------------------------------------------------------------------------
-- Stg: Stg_Offers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ABC_ANALYTICS.STAGING.STG_OFFERS (
    OFFER_id VARCHAR(64) NOT NULL UNIQUE,
    OFFER_created_DATE TIMESTAMP_TZ NOT NULL,
    booking_id VARCHAR(64) NOT NULL,
    driver_id VARCHAR(64) NOT NULL,
    route_distance NUMBER(38, 0),
    OFFER_state VARCHAR(50) NOT NULL,
    driver_read BOOLEAN NOT NULL,
    IS_VALID BOOLEAN NOT NULL,
    ERROR_REASON VARCHAR(500),
    IS_PROCESSED BOOLEAN DEFAULT FALSE NOT NULL,
    RAW_LOAD_TZ TIMESTAMP_TZ NOT NULL,
    is_missing_driver BOOLEAN, --if driver_id is absent in vw_stg_drivers
    is_missing_booking BOOLEAN --if booking_id is absent in vw_stg_bookings
);

-- -----------------------------------------------------------------------------
-- VIEW: VW_STG_OFFERS (View with valid records)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW ABC_ANALYTICS.STAGING.VW_STG_OFFERS AS
SELECT
    OFFER_id,
    OFFER_created_DATE,
    booking_id,
    driver_id,
    route_distance,
    OFFER_state,
    driver_read,
    IS_PROCESSED,
    RAW_LOAD_TZ,
    is_missing_driver,
    is_missing_booking
FROM
    ABC_ANALYTICS.STAGING.STG_OFFERS
WHERE
    IS_VALID = TRUE;

-- -----------------------------------------------------------------------------
-- DIM: Drivers (SCD Type 2 table)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE marts.dim_drivers (
    dim_driver_id NUMBER AUTOINCREMENT PRIMARY KEY,
    -- surrogate key
    driver_id VARCHAR,
    -- Natural Key (The original SHA)
    registration_date TIMESTAMP_TZ,
    driver_rating NUMBER(38, 2),
    driver_rating_count NUMBER(38, 0),
    country_name VARCHAR,
    country_code VARCHAR,
    RECEIVE_MARKETING BOOLEAN,
    -- SCD2 Columns
    start_eff_ts TIMESTAMP_TZ,
    end_eff_ts TIMESTAMP_TZ,
    is_active BOOLEAN,
    last_modified_ts TIMESTAMP_TZ
);

-- -----------------------------------------------------------------------------
-- view: vw_dim_drivers (active records)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW marts.vw_dim_drivers AS
SELECT
    dim_driver_id,
    driver_id,
    registration_date,
    dd.date_key AS registration_date_key,
    CASE
        WHEN registration_date_key <= 20210607 --can be a variable
        THEN 'Veteran'
        ELSE 'Rookie'
    END AS driver_type,
    driver_rating,
    driver_rating_count,
    country_name,
    country_code
FROM
    marts.dim_drivers d
    LEFT JOIN marts.dim_date dd ON d.registration_date :: date = dd.DATE
WHERE
    is_active = TRUE;
