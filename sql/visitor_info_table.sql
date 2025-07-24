CREATE OR REPLACE VIEW wistia_project.visitor_info AS 

WITH ranked_events AS (
    SELECT 
        visitor_id,
        country,
        ip_address,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id 
            ORDER BY event_timestamp DESC
        ) AS row_num
    FROM wistia_project.events
)

SELECT 
    visitor_id,
    country,
    ip_address
FROM ranked_events
WHERE row_num = 1;
