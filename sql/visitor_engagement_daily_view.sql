CREATE OR REPLACE VIEW wistia_project.fact_visitor_engagement_daily
AS 
WITH plays AS (
SELECT DISTINCT event_date, media_id, visitor_id,
COUNT(DISTINCT event_key) as unique_plays
FROM wistia_project.events
WHERE watched_percent > 0.01
GROUP BY media_id, event_date, visitor_id
),
loads AS (
SELECT DISTINCT 
event_date, 
media_id,
visitor_id,
COUNT(DISTINCT event_key) as unique_loads
FROM wistia_project.events
GROUP BY event_date, media_id, visitor_id
),

watch_time AS (
SELECT DISTINCT 
event_key,
event_date,
media_id,
visitor_id,
ROUND(watched_percent*duration,3) AS watch_time
FROM wistia_project.events
INNER JOIN wistia_project.media_info USING (media_id)
WHERE watched_percent > 0.01
),

watch_time_agg AS
(SELECT 
event_date,
media_id,
visitor_id,
ROUND(SUM(watch_time),3) AS watch_time
FROM watch_time
GROUP BY event_date, media_id, visitor_id
)

SELECT 
CAST(event_date AS DATE) AS event_date, 
media_id, 
visitor_id,
COALESCE(unique_plays, 0) AS unique_plays, 
unique_loads,
COALESCE(ROUND(unique_plays*1.0/unique_loads,3),0) AS play_rate,
COALESCE(watch_time, 0) AS watch_time
FROM loads 
LEFT JOIN plays USING (event_date,media_id, visitor_id )
LEFT JOIN watch_time_agg USING (event_date,media_id, visitor_id )