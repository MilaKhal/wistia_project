CREATE SCHEMA wistia_project;
CREATE EXTERNAL TABLE wistia_project.media_info 
( name string, url string, channel string, duration double, created_at timestamp, updated_at timestamp ) 
PARTITIONED BY ( media_id string ) STORED AS PARQUET LOCATION 's3://wistiabucket/curated/media_info/'