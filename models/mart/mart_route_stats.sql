   WITH route_stats AS (
    SELECT 
        f.origin AS origin_airport_code,
        ao.name AS origin_airport_name,
        ao.city AS origin_city,
        ao.country AS origin_country,
        f.dest AS destination_airport_code,
        ad.name AS destination_airport_name,
        ad.city AS destination_city,
        ad.country AS destination_country,
        COUNT(f.flight_date) AS total_flights,
        COUNT(DISTINCT f.tail_number) AS unique_airplanes,
        COUNT(DISTINCT f.airline) AS unique_airlines,
        ROUND(AVG(f.actual_elapsed_time), 2) AS avg_actual_elapsed_time,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        MAX(f.arr_delay) AS max_delay,
        MIN(f.arr_delay) AS min_delay,
        SUM(CASE WHEN f.cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN f.diverted = 1 THEN 1 ELSE 0 END) AS total_diverted
    FROM 
        {{ref('prep_flights')}}s f
    LEFT JOIN 
        {{ref('prep_airports')}} ao ON f.origin = ao.faa
    LEFT JOIN 
        {{ref('prep_airports')}} ad ON f.dest = ad.faa
    GROUP BY 
        f.origin, ao.name, ao.city, ao.country,
        f.dest, ad.name, ad.city, ad.country
)
SELECT 
    origin_airport_code,
    origin_airport_name,
    origin_city,
    origin_country,
    destination_airport_code,
    destination_airport_name,
    destination_city,
    destination_country,
    total_flights,
    unique_airplanes,
    unique_airlines,
    avg_actual_elapsed_time,
    avg_arrival_delay,
    max_delay,
    min_delay,
    total_cancelled,
    total_diverted
FROM 
    route_stats
ORDER BY 
    origin_airport_code, destination_airport_code