WITH daily_flight_stats AS (
    SELECT 
        f.origin AS airport_code,
        a.name AS airport_name,
        a.city AS airport_city,
        a.country AS airport_country,
        f.flight_date,
        COUNT(DISTINCT f.dest) AS unique_departure_connections,
        COUNT(DISTINCT f.origin) AS unique_arrival_connections,
        COUNT(f.flight_date) AS total_flights,
        SUM(CASE WHEN f.cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN f.diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
        COUNT(f.flight_date) - SUM(CASE WHEN f.cancelled = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN f.diverted = 1 THEN 1 ELSE 0 END) AS total_actual_flights,
        ROUND(AVG(f.unique_airplanes), 2) AS avg_unique_airplanes,
        ROUND(AVG(f.unique_airlines), 2) AS avg_unique_airlines
    FROM 
        {{ref('prep_flights')}} f
    LEFT JOIN 
        {{ref('prep_airports')}} a ON f.origin = a.faa
    GROUP BY 
        f.origin, a.name, a.city, a.country, f.flight_date
),
weather_stats AS (
    SELECT 
        w.airport_code,
        w.date AS weather_date,
        w.min_temp_c AS daily_min_temperature,
        w.max_temp_c AS daily_max_temperature,
        w.precipitation_mm AS daily_precipitation,
        w.max_snow_mm AS daily_snowfall,
        w.avg_wind_direction AS daily_average_wind_direction,
        w.avg_wind_speed_kmh AS daily_average_wind_speed,
        w.wind_peakgust_kmh AS daily_wind_peakgust
    FROM 
        {{ref('prep_weather_daily')}} w
),
combined_stats AS (
    SELECT 
        dfs.airport_code,
        dfs.airport_name,
        dfs.airport_city,
        dfs.airport_country,
        dfs.flight_date,
        dfs.unique_departure_connections,
        dfs.unique_arrival_connections,
        dfs.total_flights,
        dfs.total_cancelled,
        dfs.total_diverted,
        dfs.total_actual_flights,
        dfs.avg_unique_airplanes,
        dfs.avg_unique_airlines,
        ws.daily_min_temperature,
        ws.daily_max_temperature,
        ws.daily_precipitation,
        ws.daily_snowfall,
        ws.daily_average_wind_direction,
        ws.daily_average_wind_speed,
        ws.daily_wind_peakgust
    FROM 
        daily_flight_stats dfs
    LEFT JOIN 
        weather_stats ws ON dfs.airport_code = ws.airport_code AND dfs.flight_date = ws.weather_date
)
SELECT *
FROM combined_stats
ORDER BY flight_date, airport_code