-- NYC 311 Hive External Table Setup
-- Creates Hive table over S3 batch results


-- Create database
CREATE DATABASE IF NOT EXISTS nyc311_db;
USE nyc311_db;

-- External table over batch baseline results
CREATE EXTERNAL TABLE IF NOT EXISTS nyc311_baseline (
    complaint_type            STRING,
    borough                   STRING,
    hour                      INT,
    day_of_week               INT,
    historical_count          BIGINT,
    std_dev                   DOUBLE,
    avg_count_per_5min_window DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://nyc311-lambda-architecture/batch-results/baseline/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Top 5 complaint types overall
CREATE EXTERNAL TABLE IF NOT EXISTS nyc311_top5_overall (
    complaint_type STRING,
    total_count    BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://nyc311-lambda-architecture/batch-results/top5-overall/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Borough summary
CREATE EXTERNAL TABLE IF NOT EXISTS nyc311_borough_summary (
    borough           STRING,
    total_complaints  BIGINT,
    unique_types      BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://nyc311-lambda-architecture/batch-results/borough-summary/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Sample queries to verify data
SELECT complaint_type, SUM(historical_count) as total
FROM nyc311_baseline
GROUP BY complaint_type
ORDER BY total DESC
LIMIT 10;

SELECT borough, SUM(historical_count) as total
FROM nyc311_baseline
GROUP BY borough
ORDER BY total DESC;