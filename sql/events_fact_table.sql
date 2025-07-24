CREATE EXTERNAL TABLE wistia_project.events 
( media_id string, visitor_id string, event_key string, watched_percent double, event_timestamp timestamp, ip_address string, country string ) 
PARTITIONED BY (event_date string) STORED AS PARQUET LOCATION 's3://wistiabucket/curated/events/'
