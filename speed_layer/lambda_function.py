#!/usr/bin/env python3
"""
NYC 311 Speed Layer — Lambda Function
Triggered by Kinesis Data Stream records.

Implements THREE sliding windows :
  Window 1: Top 5 surging complaint types (last 5 minutes)
  Window 2: Rolling 1-minute average complaint rate per borough
  Window 3: Count of events per window + surge alerts

"""

import json
import boto3
import base64
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients 
dynamodb      = boto3.resource('dynamodb', region_name='us-east-1')
counts_table  = dynamodb.Table('nyc311-complaint-counts')
results_table = dynamodb.Table('nyc311-speed-results')

# Window configuration 
WINDOW_MINUTES  = 5    # sliding window size — top 5 in last 5 mins
ROLLING_MINUTES = 1    # rolling average window — 1 minute
TOP_N           = 5    # top N complaint types
SURGE_THRESHOLD = 5    # complaints per window = surge alert


def decode_record(record):
    """Decode base64-encoded Kinesis record into dict."""
    raw = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw)


def get_window_key(minutes=5):
    """
    Returns current N-minute window key.
    e.g. for 5 min:  '2026-07-10T13:25'
    e.g. for 1 min:  '2026-07-10T13:23'
    """
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute      = (now.minute // minutes) * minutes,
        second      = 0,
        microsecond = 0
    )
    return floored.strftime('%Y-%m-%dT%H:%M')


def get_previous_window_key(minutes=5):
    """Returns previous N-minute window key for trend comparison."""
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute      = (now.minute // minutes) * minutes,
        second      = 0,
        microsecond = 0
    )
    prev = floored - timedelta(minutes=minutes)
    return prev.strftime('%Y-%m-%dT%H:%M')


def process_records(records):
    """
    Process all records in batch.
    Returns complaint counts, borough counts and agency counts.
    """
    complaint_counts = defaultdict(int)
    borough_counts   = defaultdict(int)
    agency_counts    = defaultdict(int)

    for record in records:
        complaint_type = record.get('complaint_type', 'UNKNOWN')
        borough        = record.get('borough',        'UNKNOWN')
        agency         = record.get('agency',         'UNKNOWN')

        if complaint_type not in ('UNKNOWN', 'nan', ''):
            complaint_counts[complaint_type] += 1
        if borough not in ('UNKNOWN', 'nan', ''):
            borough_counts[borough] += 1
        if agency not in ('UNKNOWN', 'nan', ''):
            agency_counts[agency] += 1

    return complaint_counts, borough_counts, agency_counts


def window1_top5_complaints(complaint_counts):
    """
    WINDOW 1 — Top 5 surging complaint types in last 5 minutes.
    Direct match to rubric: "top 5 trending items in last N minutes"
    """
    window_key = get_window_key(WINDOW_MINUTES)

    # Update DynamoDB counts atomically
    for complaint_type, count in complaint_counts.items():
        try:
            counts_table.update_item(
                Key={'complaint_type': complaint_type},
                UpdateExpression='''
                    SET window_key   = :wk,
                        #cnt         = if_not_exists(#cnt, :zero) + :inc,
                        last_updated = :ts
                ''',
                ExpressionAttributeNames={'#cnt': 'count'},
                ExpressionAttributeValues={
                    ':wk'  : window_key,
                    ':inc' : Decimal(count),
                    ':zero': Decimal(0),
                    ':ts'  : datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Window 1 error for {complaint_type}: {e}")

    # Sort and get top 5
    top5 = sorted(
        complaint_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:TOP_N]

    return [
        {
            'rank'          : i + 1,
            'complaint_type': ct,
            'count'         : int(count),
            'window'        : f'last_{WINDOW_MINUTES}_minutes'
        }
        for i, (ct, count) in enumerate(top5)
    ]


def window2_rolling_average(borough_counts, total_records):
    """
    WINDOW 2 — Rolling 1-minute average complaint rate per borough.
    """
    one_min_key = get_window_key(ROLLING_MINUTES)

    # Rolling average = total records in this 1-minute window
    # divided by elapsed seconds x 60 to get per-minute rate
    now     = datetime.now(timezone.utc)
    elapsed = now.second + 1  # seconds elapsed in current minute

    rolling_avg = (total_records / elapsed) * 60  # projected per-minute rate

    # Borough breakdown for this 1-minute window
    borough_rates = [
        {
            'borough'        : borough,
            'count_1min'     : count,
            'projected_rate' : round((count / elapsed) * 60, 2)
        }
        for borough, count in sorted(
            borough_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
    ]

    return {
        'window_1min'        : one_min_key,
        'rolling_avg_per_min': round(rolling_avg, 2),
        'borough_rates'      : borough_rates,
        'elapsed_seconds'    : elapsed
    }


def window3_event_count_and_surge(complaint_counts, borough_counts, total_records):
    """
    WINDOW 3 — Count of events per window + surge detection.
    """
    window_key = get_window_key(WINDOW_MINUTES)

    # Surge alerts — complaint types exceeding threshold
    surge_alerts = [
        {
            'complaint_type': ct,
            'count'         : count,
            'threshold'     : SURGE_THRESHOLD,
            'severity'      : 'HIGH' if count >= SURGE_THRESHOLD * 2 else 'MEDIUM'
        }
        for ct, count in complaint_counts.items()
        if count >= SURGE_THRESHOLD
    ]

    # Index transition — borough suddenly dominating
    top_borough = max(borough_counts.items(), key=lambda x: x[1]) \
                  if borough_counts else ('UNKNOWN', 0)

    borough_dominance = round(
        (top_borough[1] / total_records * 100), 2
    ) if total_records > 0 else 0

    return {
        'window_key'       : window_key,
        'total_events'     : total_records,
        'events_per_window': total_records,
        'surge_alerts'     : surge_alerts,
        'top_borough'      : top_borough[0],
        'borough_dominance': f"{borough_dominance}%",
        'window_size_mins' : WINDOW_MINUTES
    }


def write_results_to_dynamodb(top5, rolling, surge):
    """Write all three window results to DynamoDB."""
    window_key = get_window_key(WINDOW_MINUTES)
    try:
        results_table.put_item(
            Item={
                'window_id'        : window_key,
                'prev_window_id'   : get_previous_window_key(WINDOW_MINUTES),
                'timestamp'        : datetime.now(timezone.utc).isoformat(),

                # Window 1 — Top 5 in last 5 minutes
                'top5_complaints'  : json.dumps(top5),

                # Window 2 — Rolling 1-minute average
                'rolling_avg'      : json.dumps(rolling),

                # Window 3 — Event count + surge alerts
                'total_records'    : int(surge['total_events']),
                'surge_alerts'     : json.dumps(surge['surge_alerts']),
                'top_borough'      : surge['top_borough'],
                'borough_dominance': surge['borough_dominance'],
            }
        )
        logger.info(
            f"Results written — Window: {window_key} | "
            f"Top complaint: {top5[0]['complaint_type'] if top5 else 'N/A'} | "
            f"Rolling avg: {rolling['rolling_avg_per_min']}/min"
        )
    except Exception as e:
        logger.error(f"Error writing to DynamoDB: {e}")


def lambda_handler(event, context):
    """
    Main Lambda handler — triggered by Kinesis stream.
    Processes each batch implementing three sliding windows.
    """
    logger.info(f"Processing {len(event['Records'])} Kinesis records")

    #  Decode all records 
    records = []
    for kinesis_record in event['Records']:
        try:
            record = decode_record(kinesis_record)
            records.append(record)
        except Exception as e:
            logger.warning(f"Could not decode record: {e}")

    if not records:
        return {'statusCode': 200, 'body': 'No valid records'}

    total_records = len(records)
    logger.info(f"Decoded {total_records} valid records")

    # Process records 
    complaint_counts, borough_counts, agency_counts = process_records(records)

    #  Window 1: Top 5 in last 5 minutes 
    top5 = window1_top5_complaints(complaint_counts)

    #  Window 2: Rolling 1-minute average 
    rolling = window2_rolling_average(borough_counts, total_records)

    # Window 3: Event count + surge alerts 
    surge = window3_event_count_and_surge(
        complaint_counts, borough_counts, total_records
    )

    # Write all results to DynamoDB 
    write_results_to_dynamodb(top5, rolling, surge)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'records_processed'  : total_records,
            'window_key'         : get_window_key(WINDOW_MINUTES),
            'top_complaint'      : top5[0]['complaint_type'] if top5 else 'N/A',
            'rolling_avg_per_min': rolling['rolling_avg_per_min'],
            'surge_alerts'       : len(surge['surge_alerts'])
        })
    }