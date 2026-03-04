USE DATABASE ABC_ANALYTICS;

USE schema marts;

-- -----------------------------------------------------------------------------
-- DIM: Date
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_date AS WITH dates AS (
  SELECT
    DATEADD(DAY, seq4(), '2021-06-01') AS date_day
  FROM
    TABLE(GENERATOR(ROWCOUNT = > 30)) -- from 2021-06-01 to 2021-06-30
)

SELECT
  to_date(date_day) AS "DATE",
  TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) :: INT AS DATE_KEY,
  YEAR(date_day) AS YEAR,
  MONTH(date_day) AS MONTH,
  TO_CHAR(date_day, 'MMMM') AS MONTH_NAME,
  -- full month name
  DAY(date_day) AS DAY_OF_MONTH,
  DAYOFWEEK(date_day) AS DAY_OF_WEEK_NUM,
  -- 0=Sunday, 6=Saturday
  dayname(date_day) AS DAY_NAME,
  --  day name short
  CASE
    WHEN DAYOFWEEK(date_day) IN (0, 6) THEN 1
    ELSE 0
  END AS WEEKEND_FLAG,
  -- 1=weekend Sat(6),Sun(0)
  QUARTER(date_day) AS QUARTER,
  TO_CHAR(date_day, 'YYYY') || '-Q' || QUARTER(date_day) AS YEAR_QUARTER
FROM
  dates
WHERE
  date_day BETWEEN '2020-01-01'
  AND '2030-12-31'
ORDER BY
  date_day;