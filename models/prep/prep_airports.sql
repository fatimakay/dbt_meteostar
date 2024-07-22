WITH airports_reorder AS (
    select
    faa,
    name,
    country, 
    region,
    lat, lon, alt, tz, dst, city
    from {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder