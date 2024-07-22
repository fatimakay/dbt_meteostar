WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT 
		 timestamp::DATE AS date, -- only time (hours:minutes:seconds) as TIME data type
		 timestamp::TIME AS time, -- only time (hours:minutes:seconds) as TIME data type
         TO_CHAR(timestamp,'HH24:MI') as hour, -- time (hours:minutes) as TEXT data type
         TO_CHAR(timestamp, 'FMmonth') AS month, -- month name as a text
         TO_CHAR(timestamp, 'day') AS weekday, -- weekday name as text        
         DATE_PART('day', timestamp) AS date_day,
		 DATE_PART('month', timestamp) AS date_month,
		 DATE_PART('year', timestamp) AS date_year,
		 DATE_PART('week', timestamp) as calendar_week,
		*
    FROM hourly_data
),
add_more_features AS (
    SELECT 
		(CASE 
			WHEN time BETWEEN '00:00' AND '5:59' THEN 'night'
			WHEN time between '6:00' and '17:59' THEN 'day'
			when time between '18:00' and '23:59' THEN 'evening'
		END) AS day_part, *
    FROM add_features
)
SELECT *
FROM add_more_features