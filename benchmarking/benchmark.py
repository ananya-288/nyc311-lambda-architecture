#!/usr/bin/env python3
"""
NYC 311 Benchmarking Script
Measures EMR batch job execution time across different worker counts.
Records throughput, latency, and speedup for Phase 3 performance analysis.
"""

import boto3
import time
import json
import csv
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Configuration 
REGION      = 'us-east-1'
BUCKET      = 'nyc311-lambda-architecture'
SCRIPT_PATH = 's3://nyc311-lambda-architecture/scripts/spark_job.py'
LOG_URI     = 's3://nyc311-lambda-architecture/emr-logs/'

emr = boto3.client('emr', region_name=REGION)


def create_emr_cluster(num_workers):
    """Launch EMR cluster with specified worker count."""
    logger.info(f"Launching EMR cluster with {num_workers} workers...")

    response = emr.run_job_flow(
        Name=f'NYC311-Benchmark-{num_workers}workers-{datetime.now().strftime("%H%M")}',
        ReleaseLabel='emr-6.5.0',
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Spark'},
            {'Name': 'Hive'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Workers',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': num_workers,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,  # Auto-terminate 
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'NYC311-Spark-Batch-Job',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        SCRIPT_PATH
                    ]
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=LOG_URI,
        AutoTerminationPolicy={'IdleTimeout': 3600},  # Terminate after 1 hour 
        )

    cluster_id = response['JobFlowId']
    logger.info(f"Cluster launched: {cluster_id}")
    return cluster_id


def wait_for_completion(cluster_id):
    """Wait for EMR cluster to complete and return duration."""
    start_time = time.time()
    logger.info(f"Waiting for cluster {cluster_id} to complete...")

    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        status   = response['Cluster']['Status']['State']
        logger.info(f"Cluster status: {status}")

        if status in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            elapsed = time.time() - start_time
            logger.info(f"Cluster finished in {elapsed:.0f} seconds")
            return elapsed, status

        time.sleep(30)  # Check every 30 seconds


def run_benchmark():
    """Run benchmark across different worker counts."""
    worker_counts = [1, 2, 3, 4]
    results       = []

  
    logger.info("NYC 311 EMR BENCHMARK")
   
    for num_workers in worker_counts:
        logger.info(f"\nBenchmark run: {num_workers} worker(s)")

        cluster_id        = create_emr_cluster(num_workers)
        elapsed, status   = wait_for_completion(cluster_id)

        result = {
            'workers'    : num_workers,
            'duration_s' : elapsed,
            'status'     : status,
            'timestamp'  : datetime.now().isoformat()
        }
        results.append(result)

        logger.info(f"Workers: {num_workers} | Time: {elapsed:.0f}s | Status: {status}")

        # Wait between runs to avoid throttling
        if num_workers < worker_counts[-1]:
            logger.info("Waiting 60s before next run...")
            time.sleep(60)

    # Calculate speedup 
    baseline_time = results[0]['duration_s']  # 1 worker = baseline

  
    logger.info("BENCHMARK RESULTS")
    logger.info(f"{'Workers':<10} {'Time (s)':<12} {'Speedup':<10} {'Status'}")

    for r in results:
        speedup = baseline_time / r['duration_s']
        r['speedup'] = speedup
        logger.info(
            f"{r['workers']:<10} {r['duration_s']:<12.0f} "
            f"{speedup:<10.2f} {r['status']}"
        )

    # Save results 
    output_file = 'benchmark_results.csv'
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"\nResults saved to: {output_file}")
    return results


if __name__ == '__main__':
    run_benchmark()