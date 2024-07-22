WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT 
	DATE_PART('month', date) as date_month, 
	TO_CHAR(date, 'Day') as weekday,
	DATE_PART('day', date) as date_day, 
	TRIM(TO_CHAR(date, 'Month')) as month, -- Trim spaces
	DATE_PART('year', date) as year,
	DATE_PART('week', date) as calendar_week,
	*
    FROM daily_data 
),
add_more_features AS (
    SELECT 
		 (CASE 
			WHEN month in ('December', 'January', 'February') THEN 'winter'
			WHEN month in ('March', 'April', 'May') THEN 'spring'
            WHEN month in ('June', 'July', 'August') THEN 'summer'
            when month in ('September', 'October', 'November') then 'autumn'
		END) AS season, *
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date