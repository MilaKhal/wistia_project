# Wistia Event Analytics Pipeline and Dashboard

## Project Description & Design Overview

This project implements a **data pipeline and dashboard** to ingest, process, and visualize event and media metadata from the Wistia API using AWS services and Streamlit.

<img width="898" height="192" alt="image" src="https://github.com/user-attachments/assets/5934fc68-9664-4cc4-b64c-10fb502e62f6" />


### 1. GitHub Actions – CI/CD Automation  
- **Purpose:** Automates deployment of pipeline code, including AWS Glue jobs.  
- **Why:** Ensures consistent, version-controlled updates whenever new code is pushed to the repository.

### 2. AWS Glue + Python  
- **Purpose:** Executes Python and PySpark scripts to ingest both initial and incremental event and media metadata from the Wistia API.  
- **Why:** Provides a lightweight, fully managed, serverless environment for ETL processing.

### 3. Amazon S3 (Raw Layer) – Storage of Raw Data  
- **Purpose:** Stores raw JSON files fetched from Wistia, organized by media ID and ingestion date.  
- **Why:** Acts as a durable, cost-effective landing zone for all incoming raw data.

### 4. AWS Glue (PySpark) – Data Transformation  
- **Purpose:** Reads raw data from S3, deduplicates records, normalizes fields, and prepares datasets for analytics.  
- **Why:** Enables scalable and distributed data processing without managing infrastructure.

### 5. Amazon S3 (Curated Layer) – Cleaned Data Storage  
- **Purpose:** Stores the cleaned, partitioned, and optimized Parquet files output from Glue jobs.  
- **Why:** Provides a query-ready, efficient data layer for analytics.

### 6. Glue Data Catalog – Metadata Registry  
- **Purpose:** Defines table schemas and partitions for curated datasets stored in S3.  
- **Why:** Makes data discoverable and queryable by Athena and other AWS analytics services.

### 7. Amazon Athena – SQL Query Engine  
- **Purpose:** Executes serverless SQL queries on the curated data directly in S3 using Glue Catalog metadata.  
- **Why:** Enables cost-effective, on-demand analytics without data movement.
  <img width="634" height="477" alt="image" src="https://github.com/user-attachments/assets/f626eb15-694a-41c7-8418-c23effee5145" />


### 8. Streamlit – Interactive Dashboard  
- **Purpose:** Offers a Python-based, interactive front end to explore Wistia engagement metrics and insights.  
- **Why:** Allows users to visualize real-time analytics by running Athena queries via an intuitive UI.

---

## Folder Structure

