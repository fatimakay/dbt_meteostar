WITH weekly_weather AS (
    SELECT
        DATE_TRUNC('week', date) AS week_start, -- Truncates the date to the start of the week
        airport_code,
        MIN(min_temp_c) AS min_temp, 
        MAX(max_temp_c) AS max_temp, 
        SUM(precipitation_mm) AS total_precipitation, 
        SUM(max_snow_mm) AS total_snowfall, 
        AVG(avg_wind_direction) AS avg_wind_direction, 
        AVG(avg_wind_speed_kmh) AS avg_wind_speed, 
        MAX(wind_peakgust_kmh) AS max_wind_gust
    FROM
        prep_weather_daily
    GROUP BY
        DATE_TRUNC('week', date), airport_code -- Group by week and airport
)
SELECT *
FROM weekly_weather
ORDER BY week_start, airport_code