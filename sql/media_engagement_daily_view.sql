CREATE OR REPLACE VIEW wistia_project.fact_media_engagement_daily AS 

WITH plays AS (
SELECT DISTINCT event_date, media_id,
COUNT(DISTINCT visitor_id) as unique_plays
FROM wistia_project.events
WHERE watched_percent > 0.01
GROUP BY media_id, event_date
),
loads AS (
SELECT DISTINCT 
event_date, 
media_id,
COUNT(DISTINCT visitor_id) as unique_loads
FROM wistia_project.events
GROUP BY event_date, media_id
)
SELECT 
CAST(event_date AS DATE) AS event_date, 
media_id, 
unique_plays, 
unique_loads,
ROUND(unique_plays*1.0/unique_loads,3) AS play_rate 
FROM loads 
LEFT JOIN plays USING (event_date,media_id )