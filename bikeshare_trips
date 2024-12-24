/*
 * Analysis of NYC Citibike Trips with Weather Data
 * 
 * This query analyzes Citibike trip patterns in relation to weather conditions
 * by joining trip data with NOAA weather data and ZIP code geographical information.
 * It aggregates trips into 10-minute intervals and includes geographical context
 * for both start and end locations.
 */

-- Select core trip attributes and calculate derived metrics
SELECT
    -- User and location information
    TRI.usertype,
    ZIPSTART.zip_code AS zip_code_start,
    ZIPSTARTNAME.borough AS borough_start,
    ZIPSTARTNAME.neighborhood AS neighborhood_start,
    ZIPEND.zip_code AS zip_code_end,
    ZIPENDNAME.borough AS borough_end,
    ZIPENDNAME.neighborhood AS neighborhood_end,
    
    -- Trip timing information
    DATE(TRI.starttime) AS start_day,
    DATE(TRI.stoptime) AS stop_day,
    
    -- Weather conditions for the trip day
    WEA.temp AS day_mean_temperature,            -- Average temperature in Fahrenheit
    WEA.wdsp AS day_mean_wind_speed,            -- Average wind speed in knots
    WEA.prcp AS day_total_precipitation,         -- Total precipitation in inches
    
    -- Trip duration rounded to 10-minute intervals for better aggregation
    ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes,
    COUNT(TRI.bikeid) AS trip_count

-- Main data sources and joins
FROM
    -- Base trip data from Citibike system
    `bigquery-public-data.new_york_citibike.citibike_trips` AS TRI

    -- Join with ZIP code boundaries to get start location context
    INNER JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
        ON ST_WITHIN(
            ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
            ZIPSTART.zip_code_geom)

    -- Join with ZIP code boundaries to get end location context
    INNER JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPEND
        ON ST_WITHIN(
            ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
            ZIPEND.zip_code_geom)

    -- Join with weather data for the trip day
    INNER JOIN `bigquery-public-data.noaa_gsod.gsod20*` AS WEA
        ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)

    -- Join with ZIP code metadata for start location
    INNER JOIN `striped-booking-442812-m0.cyclistic.zip_codes` AS ZIPSTARTNAME
        ON ZIPSTART.zip_code = CAST(ZIPSTARTNAME.zip AS STRING)

    -- Join with ZIP code metadata for end location
    INNER JOIN `striped-booking-442812-m0.cyclistic.zip_codes` AS ZIPENDNAME
        ON ZIPEND.zip_code = CAST(ZIPENDNAME.zip AS STRING)

-- Filter conditions
WHERE
    WEA.wban = '94728'                          -- Limit to Central Park weather station
    AND EXTRACT(YEAR FROM DATE(TRI.starttime)) BETWEEN 2014 AND 2015  -- Only 2 years of (data 2014-2015)

-- Group by all non-aggregated columns
GROUP BY
    usertype,
    zip_code_start,
    borough_start,
    neighborhood_start,
    zip_code_end,
    borough_end,
    neighborhood_end,
    start_day,
    stop_day,
    day_mean_temperature,
    day_mean_wind_speed,
    day_total_precipitation,
    trip_minutes

-- Order results by date, most recent first
ORDER BY
    start_day DESC
