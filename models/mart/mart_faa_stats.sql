WITH airport_stats AS (
    SELECT 
        pa.faa AS airport_code,
        pa.name AS airport_name,
        pa.city,
        pa.country,
        COUNT(DISTINCT CASE WHEN pf.origin = pa.faa THEN pf.dest END) AS unique_departure_connections,
        COUNT(DISTINCT CASE WHEN pf.dest = pa.faa THEN pf.origin END) AS unique_arrival_connections,
        COUNT(pf.flight_date) AS total_planned_flights,
        SUM(CASE WHEN pf.cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled_flights,
        SUM(CASE WHEN pf.diverted = 1 THEN 1 ELSE 0 END) AS total_diverted_flights,
        SUM(CASE WHEN pf.cancelled = 0 AND pf.diverted = 0 THEN 1 ELSE 0 END) AS total_actual_flights,
        COUNT(DISTINCT CASE WHEN pf.origin = pa.faa OR pf.dest = pa.faa THEN pf.tail_number END) AS unique_airplanes,
        COUNT(DISTINCT CASE WHEN pf.origin = pa.faa OR pf.dest = pa.faa THEN pf.airline END) AS unique_airlines
    FROM 
        prep_airports pa
    LEFT JOIN 
        prep_flights pf ON pa.faa = pf.origin OR pa.faa = pf.dest
    GROUP BY 
        pa.faa, pa.name, pa.city, pa.country
)
SELECT 
    airport_code,
    airport_name,
    city,
    country,
    unique_departure_connections,
    unique_arrival_connections,
    total_planned_flights,
    total_cancelled_flights,
    total_diverted_flights,
    total_actual_flights,
     ROUND(CAST(unique_airplanes AS numeric) / NULLIF(total_actual_flights, 0), 2) AS avg_unique_airplanes,
    ROUND(CAST(unique_airlines AS numeric) / NULLIF(total_actual_flights, 0), 2) AS avg_unique_airlines
FROM 
    airport_stats
ORDER BY 
    airport_code;