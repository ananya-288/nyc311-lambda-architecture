#!/usr/bin/env python3
"""
NYC 311 Speed Layer — Lambda Function
Triggered by Kinesis Data Stream records.

Implements THREE sliding windows:
  Window 1: Top 5 surging complaint types (last 5 minutes)
            Uses composite DynamoDB key so counts reset per window
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
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb      = boto3.resource('dynamodb', region_name='us-east-1')
counts_table  = dynamodb.Table('nyc311-complaint-counts')
results_table = dynamodb.Table('nyc311-speed-results')

# Window configuration
WINDOW_MINUTES  = 5
ROLLING_MINUTES = 1
TOP_N           = 5
SURGE_THRESHOLD = 5


def decode_record(record):
    """Decode base64-encoded Kinesis record into dict."""
    raw = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw)


def get_window_key(minutes=5):
    """Returns current N-minute window key."""
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute=(now.minute // minutes) * minutes,
        second=0,
        microsecond=0
    )
    return floored.strftime('%Y-%m-%dT%H:%M')


def get_previous_window_key(minutes=5):
    """Returns previous N-minute window key."""
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute=(now.minute // minutes) * minutes,
        second=0,
        microsecond=0
    )
    return (floored - timedelta(minutes=minutes)).strftime('%Y-%m-%dT%H:%M')


def process_records(records):
    """Process all records in batch."""
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
    Uses composite key (window_id, complaint_type) so counts
    reset naturally when a new 5-minute window starts.
    """
    window_key = get_window_key(WINDOW_MINUTES)

    # Accumulate counts in DynamoDB for this window
    for complaint_type, count in complaint_counts.items():
        try:
            counts_table.update_item(
                Key={
                    'window_id'     : window_key,
                    'complaint_type': complaint_type
                },
                UpdateExpression='''
                    SET #cnt         = if_not_exists(#cnt, :zero) + :inc,
                        last_updated = :ts
                ''',
                ExpressionAttributeNames={'#cnt': 'count'},
                ExpressionAttributeValues={
                    ':inc' : Decimal(count),
                    ':zero': Decimal(0),
                    ':ts'  : datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Window 1 error for {complaint_type}: {e}")

    # Read back ALL accumulated counts for this window
    try:
        response = counts_table.query(
            KeyConditionExpression=Key('window_id').eq(window_key)
        )
        window_totals = {
            item['complaint_type']: int(item['count'])
            for item in response['Items']
        }
    except Exception as e:
        logger.error(f"Error reading window totals: {e}")
        window_totals = dict(complaint_counts)

    # Sort and get top 5 from full window accumulation
    top5 = sorted(
        window_totals.items(),
        key=lambda x: x[1],
        reverse=True
    )[:TOP_N]

    return [
        {
            'rank'          : i + 1,
            'complaint_type': ct,
            'count'         : c,
            'window'        : f'last_{WINDOW_MINUTES}_minutes'
        }
        for i, (ct, c) in enumerate(top5)
    ]


def window2_rolling_average(borough_counts, total_records):
    """
    WINDOW 2 — Rolling 1-minute average complaint rate per borough.
    """
    one_min_key = get_window_key(ROLLING_MINUTES)
    now         = datetime.now(timezone.utc)
    elapsed     = now.second + 1
    rolling_avg = (total_records / elapsed) * 60

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
    """Write all three window results to DynamoDB.
    """
    window_key = get_window_key(WINDOW_MINUTES)
    try:
        results_table.put_item(
            Item={
                'window_id'        : window_key,
                'prev_window_id'   : get_previous_window_key(WINDOW_MINUTES),
                'timestamp'        : datetime.now(timezone.utc).isoformat(),
                'top5_complaints'  : json.dumps(top5),
                'rolling_avg'      : json.dumps(rolling),
                'total_records'    : int(surge['total_events']),
                'surge_alerts'     : json.dumps(surge['surge_alerts']),
                'top_borough'      : surge['top_borough'],
                'borough_dominance': surge['borough_dominance'],
            }
        )
        logger.info(
            f"Results written — Window: {window_key} | "
            f"Top: {top5[0]['complaint_type'] if top5 else 'N/A'} | "
            f"Rolling avg: {rolling['rolling_avg_per_min']}/min"
        )

        # SNS alert when surge detected — Lab 08 pattern
        if surge['surge_alerts']:
            sns_client = boto3.client('sns', region_name='us-east-1')

            # Build human readable message
            alert_lines = []
            for alert in surge['surge_alerts']:
                alert_lines.append(
                    f"  - {alert['complaint_type']}: "
                    f"{alert['count']} complaints "
                    f"[{alert['severity']} severity]"
                )

            message = (
                f"NYC 311 COMPLAINT SURGE ALERT\n"
                f"Time Window:   {window_key}\n"
                f"Top Borough:   {surge['top_borough']} "
                f"({surge['borough_dominance']} of complaints)\n"
                f"Total Records: {int(surge['total_events'])} in this window\n\n"
                f"SURGES DETECTED:\n"
                f"{chr(10).join(alert_lines)}\n\n"
                f"Generated by NYC 311 Lambda Architecture System\n"
                f"Speed Layer: AWS Lambda + DynamoDB\n"
                f"Batch Layer: AWS EMR + PySpark\n"
                f"Dataset: NYC Open Data 311 Service Requests\n"
            )

            sns_client.publish(
                TopicArn='arn:aws:sns:us-east-1:510422474327:nyc311-surge-alerts',
                Subject=(
                    f'NYC 311 Surge Alert — '
                    f'{surge["top_borough"]} | '
                    f'{len(surge["surge_alerts"])} surges detected'
                ),
                Message=message
            )
            logger.info(
                f"SNS alert published — "
                f"{len(surge['surge_alerts'])} surges detected"
            )

    except Exception as e:
        logger.error(f"Error writing results: {e}")

def lambda_handler(event, context):
    """Main Lambda handler — triggered by Kinesis stream."""
    logger.info(f"Processing {len(event['Records'])} Kinesis records")

    records = []
    for kinesis_record in event['Records']:
        try:
            record = decode_record(kinesis_record)
            records.append(record)
        except Exception as e:
            logger.warning(f"Could not decode record: {e}")

    if not records:
        return {'statusCode': 200, 'body': 'No valid records'}

    total_records                                    = len(records)
    complaint_counts, borough_counts, agency_counts  = process_records(records)

    top5    = window1_top5_complaints(complaint_counts)
    rolling = window2_rolling_average(borough_counts, total_records)
    surge   = window3_event_count_and_surge(
        complaint_counts, borough_counts, total_records
    )

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