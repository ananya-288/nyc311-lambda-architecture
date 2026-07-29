#!/usr/bin/env python3
"""
NYC 311 Batch Layer — PySpark Job
Computes historical complaint baselines from full dataset.
Uses both PySpark DataFrame API and Spark SQL.
Runs on AWS EMR cluster.

Output: complaint_type, borough, hour, day_of_week,
        historical_count, std_dev, avg_count_per_5min_window

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_INPUT  = 's3://nyc311-lambda-architecture/raw-data/nyc311_clean.csv'
S3_OUTPUT = 's3://nyc311-lambda-architecture/batch-results/baseline/'


def main():
    # Start Spark session
    spark = SparkSession.builder \
        .appName("NYC311_Batch_Baseline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Reading from: {S3_INPUT}")

    # Read dataset 
    df = spark.read.csv(S3_INPUT, header=True, inferSchema=True)
    logger.info(f"Total records: {df.count():,}")
    logger.info(f"Columns: {df.columns}")

    # Parse datetime features 
    df = df.withColumn('created_ts',  F.to_timestamp('created_date'))
    df = df.withColumn('hour',        F.hour('created_ts'))
    df = df.withColumn('day_of_week', F.dayofweek('created_ts'))
    df = df.withColumn('month',       F.month('created_ts'))

    # Filter nulls 
    df = df.filter(
        F.col('complaint_type').isNotNull() &
        F.col('borough').isNotNull() &
        F.col('hour').isNotNull() &
        F.col('day_of_week').isNotNull()
    )

    # Calculate date range for per-window average 
    date_range = df.agg(
        F.min('created_ts').alias('min_d'),
        F.max('created_ts').alias('max_d')
    ).collect()[0]

    total_days             = (date_range['max_d'] - date_range['min_d']).days
    matching_days_per_week = total_days / 7.0
    windows_per_hour       = 12

    logger.info(f"Date range: {date_range['min_d']} to {date_range['max_d']}")
    logger.info(f"Total days: {total_days}")

    # PART A: PySpark DataFrame API 
    # Compute historical baseline per complaint+borough+hour+day
    logger.info("Computing baseline using PySpark DataFrame API...")

    baseline = df.groupBy(
        'complaint_type',
        'borough',
        'hour',
        'day_of_week'
    ).agg(
        F.count('*').alias('historical_count'),
        F.stddev(F.lit(1)).alias('std_dev')
    )

    # Add avg_count_per_5min_window — comparable to speed layer
    baseline = baseline.withColumn(
        'avg_count_per_5min_window',
        F.col('historical_count') / (matching_days_per_week * windows_per_hour)
    )

    logger.info(f"Baseline combinations: {baseline.count():,}")

    # PART B: Spark SQL
    # Register temp view and run SQL queries
    logger.info("Running Spark SQL analysis...")
    df.createOrReplaceTempView("nyc311_complaints")

    # Top 5 complaint types overall
    top5_sql = spark.sql("""
        SELECT complaint_type,
               COUNT(*) as total_count
        FROM nyc311_complaints
        WHERE complaint_type IS NOT NULL
        AND borough IS NOT NULL
        GROUP BY complaint_type
        ORDER BY total_count DESC
        LIMIT 5
    """)
    logger.info("Top 5 complaint types via Spark SQL:")
    top5_sql.show(truncate=False)

    # Top borough by complaint volume
    top_boroughs = spark.sql("""
        SELECT borough,
               COUNT(*) as total_complaints,
               COUNT(DISTINCT complaint_type) as unique_types
        FROM nyc311_complaints
        WHERE borough IS NOT NULL
        AND borough != 'Unspecified'
        GROUP BY borough
        ORDER BY total_complaints DESC
    """)
    logger.info("Borough summary via Spark SQL:")
    top_boroughs.show(truncate=False)

    # Peak hours analysis
    peak_hours = spark.sql("""
        SELECT hour,
               COUNT(*) as complaint_count,
               COUNT(DISTINCT complaint_type) as unique_types
        FROM nyc311_complaints
        WHERE hour IS NOT NULL
        GROUP BY hour
        ORDER BY complaint_count DESC
        LIMIT 5
    """)
    logger.info("Peak hours via Spark SQL:")
    peak_hours.show(truncate=False)

    # Save Spark SQL results to S3
    top5_sql.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv('s3://nyc311-lambda-architecture/batch-results/top5-overall/')

    top_boroughs.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv('s3://nyc311-lambda-architecture/batch-results/borough-summary/')

    # Write main baseline to S3 
    logger.info(f"Writing baseline to: {S3_OUTPUT}")
    baseline.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(S3_OUTPUT)

    logger.info("Batch job complete!")
    spark.stop()


if __name__ == '__main__':
    main()