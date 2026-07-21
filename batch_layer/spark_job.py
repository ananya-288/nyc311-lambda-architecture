#!/usr/bin/env python3
"""
NYC 311 Batch Layer — PySpark Job
Computes historical complaint baselines from full dataset.
Runs on AWS EMR cluster.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration 
S3_INPUT  = 's3://nyc311-lambda-architecture/raw-data/nyc311_clean.csv'
S3_OUTPUT = 's3://nyc311-lambda-architecture/batch-results/baseline/'

def main():
    # Start Spark session 
    spark = SparkSession.builder \
        .appName("NYC311_Batch_Baseline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session started")
    logger.info(f"Reading from: {S3_INPUT}")

    # Read dataset 
    df = spark.read.csv(
        S3_INPUT,
        header=True,
        inferSchema=True
    )

    logger.info(f"Total records: {df.count():,}")
    logger.info(f"Columns: {df.columns}")

    # Parse datetime features 
    df = df.withColumn(
        'created_ts',
        F.to_timestamp('created_date')
    )
    df = df.withColumn('hour',        F.hour('created_ts'))
    df = df.withColumn('day_of_week', F.dayofweek('created_ts'))
    df = df.withColumn('month',       F.month('created_ts'))

    # Filter out nulls 
    df = df.filter(
        F.col('complaint_type').isNotNull() &
        F.col('borough').isNotNull() &
        F.col('hour').isNotNull() &
        F.col('day_of_week').isNotNull()
    )

    # Compute historical baseline 
    # Group by complaint_type + borough + hour + day_of_week
    # This gives  the historical pattern per combination
    baseline = df.groupBy(
        'complaint_type',
        'borough',
        'hour',
        'day_of_week'
    ).agg(
        F.count('*').alias('historical_count'),
        F.avg(F.lit(1)).alias('avg_per_window'),
        F.stddev(F.lit(1)).alias('std_dev')
    )

    logger.info(f"Baseline combinations: {baseline.count():,}")

    # Top 5 complaint types ALL TIME per borough
    top5_alltime = df.groupBy('borough', 'complaint_type') \
        .count() \
        .orderBy('borough', F.desc('count'))

    top5_alltime.write.mode('overwrite') \
        .option('header', 'true') \
        .csv('s3://nyc311-lambda-architecture/batch-results/top5-alltime/')

    # Top 5 overall across entire dataset
    top5_overall = df.groupBy('complaint_type') \
        .count() \
        .orderBy(F.desc('count')) \
        .limit(5)

    top5_overall.write.mode('overwrite') \
        .option('header', 'true') \
        .csv('s3://nyc311-lambda-architecture/batch-results/top5-overall/')

if __name__ == '__main__':
    main()