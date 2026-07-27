#!/usr/bin/env python3
"""
NYC 311 Batch Layer — PySpark Job
Computes historical complaint baselines from full dataset.
Runs on AWS EMR cluster.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_INPUT  = 's3://nyc311-lambda-architecture/raw-data/nyc311_clean.csv'
S3_OUTPUT = 's3://nyc311-lambda-architecture/batch-results/baseline/'


def main():
    spark = SparkSession.builder \
        .appName("NYC311_Batch_Baseline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Reading from: {S3_INPUT}")

    # Read dataset
    df = spark.read.csv(S3_INPUT, header=True, inferSchema=True)
    logger.info(f"Total records: {df.count():,}")

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
    windows_per_hour       = 12  # 60 mins / 5 min window

    logger.info(f"Date range: {date_range['min_d']} to {date_range['max_d']}")
    logger.info(f"Total days: {total_days}")
    logger.info(f"Matching days/week: {matching_days_per_week:.1f}")

    # Compute historical baseline
    baseline = df.groupBy(
        'complaint_type',
        'borough',
        'hour',
        'day_of_week'
    ).agg(
        F.count('*').alias('historical_count'),
        F.stddev(F.lit(1)).alias('std_dev')
    )

    # Add avg_count_per_5min_window
    # This is directly comparable to speed layer's 5-minute count
    baseline = baseline.withColumn(
        'avg_count_per_5min_window',
        F.col('historical_count') / (matching_days_per_week * windows_per_hour)
    )

    logger.info(f"Baseline combinations: {baseline.count():,}")

    # Show top 5 for validation
    top5 = df.groupBy('complaint_type') \
              .count() \
              .orderBy(F.desc('count')) \
              .limit(5)
    logger.info("Top 5 complaint types overall:")
    top5.show(truncate=False)

    # Write baseline to S3
    logger.info(f"Writing baseline to: {S3_OUTPUT}")
    baseline.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(S3_OUTPUT)

    logger.info("Batch job complete!")
    spark.stop()


if __name__ == '__main__':
    main()