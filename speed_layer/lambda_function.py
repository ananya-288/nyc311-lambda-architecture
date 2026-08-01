#!/usr/bin/env python3
"""
NYC 311 Speed Layer — Lambda Function
Triggered by Kinesis Data Stream records.

Implements THREE sliding windows — all accumulating across full 5-minute window:
  Window 1: Top 5 surging complaint types (last 5 minutes)
  Window 2: Rolling average complaint rate per borough (last 5 minutes)
  Window 3: Total event count per window + surge alerts

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

dynamodb      = boto3.resource('dynamodb', region_name='us-east-1')
counts_table  = dynamodb.Table('nyc311-complaint-counts')
results_table = dynamodb.Table('nyc311-speed-results')

WINDOW_MINUTES  = 5
TOP_N           = 5
SURGE_THRESHOLD = 5

# Special prefix for borough keys stored in counts_table
BOROUGH_PREFIX = '__borough__'
TOTAL_PREFIX   = '__total__'


def decode_record(record):
    raw = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw)


def get_window_key(minutes=5):
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute=(now.minute // minutes) * minutes,
        second=0,
        microsecond=0
    )
    return floored.strftime('%Y-%m-%dT%H:%M')


def get_previous_window_key(minutes=5):
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute=(now.minute // minutes) * minutes,
        second=0,
        microsecond=0
    )
    return (floored - timedelta(minutes=minutes)).strftime('%Y-%m-%dT%H:%M')


def process_records(records):
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


def accumulate_to_dynamodb(window_key, complaint_counts, borough_counts, total_records):
    """
    Accumulate ALL counts to DynamoDB for the current window.
    Uses special key prefixes to distinguish complaint, borough, and total counts.
    """
    # Accumulate complaint type counts
    for complaint_type, count in complaint_counts.items():
        try:
            counts_table.update_item(
                Key={
                    'window_id'     : window_key,
                    'complaint_type': complaint_type
                },
                UpdateExpression='SET #cnt = if_not_exists(#cnt, :zero) + :inc, last_updated = :ts',
                ExpressionAttributeNames={'#cnt': 'count'},
                ExpressionAttributeValues={
                    ':inc' : Decimal(count),
                    ':zero': Decimal(0),
                    ':ts'  : datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Error accumulating complaint {complaint_type}: {e}")

    # Accumulate borough counts with special prefix
    for borough, count in borough_counts.items():
        try:
            counts_table.update_item(
                Key={
                    'window_id'     : window_key,
                    'complaint_type': f'{BOROUGH_PREFIX}{borough}'
                },
                UpdateExpression='SET #cnt = if_not_exists(#cnt, :zero) + :inc, last_updated = :ts',
                ExpressionAttributeNames={'#cnt': 'count'},
                ExpressionAttributeValues={
                    ':inc' : Decimal(count),
                    ':zero': Decimal(0),
                    ':ts'  : datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Error accumulating borough {borough}: {e}")

    # Accumulate total event count with special prefix
    try:
        counts_table.update_item(
            Key={
                'window_id'     : window_key,
                'complaint_type': f'{TOTAL_PREFIX}events'
            },
            UpdateExpression='SET #cnt = if_not_exists(#cnt, :zero) + :inc, last_updated = :ts',
            ExpressionAttributeNames={'#cnt': 'count'},
            ExpressionAttributeValues={
                ':inc' : Decimal(total_records),
                ':zero': Decimal(0),
                ':ts'  : datetime.now(timezone.utc).isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Error accumulating total: {e}")


def read_window_totals(window_key):
    """
    Read ALL accumulated counts for the current window from DynamoDB.
    Returns complaint totals, borough totals, and event total.
    """
    try:
        response = counts_table.query(
            KeyConditionExpression=Key('window_id').eq(window_key)
        )
        items = response.get('Items', [])

        complaint_totals = {}
        borough_totals   = {}
        event_total      = 0

        for item in items:
            ct    = item['complaint_type']
            count = int(item['count'])

            if ct.startswith(BOROUGH_PREFIX):
                borough = ct.replace(BOROUGH_PREFIX, '')
                borough_totals[borough] = count
            elif ct.startswith(TOTAL_PREFIX):
                event_total = count
            else:
                complaint_totals[ct] = count

        return complaint_totals, borough_totals, event_total

    except Exception as e:
        logger.error(f"Error reading window totals: {e}")
        return {}, {}, 0


def window1_top5_complaints(complaint_totals):
    """
    WINDOW 1 — Top 5 surging complaint types accumulated across full 5-minute window.
    """
    top5 = sorted(
        complaint_totals.items(),
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


def window2_rolling_average(borough_totals, event_total):
    """
    WINDOW 2 — Rolling average complaint rate per borough across full 5-minute window.
    All counts accumulated across the entire window — not just current batch.
    """
    window_key = get_window_key(WINDOW_MINUTES)

    # Rolling avg = total events in window / window duration in minutes
    rolling_avg = round(event_total / WINDOW_MINUTES, 2) if event_total > 0 else 0

    borough_rates = [
        {
            'borough'        : borough,
            'count_5min'     : count,
            'projected_rate' : round(count / WINDOW_MINUTES, 2)
        }
        for borough, count in sorted(
            borough_totals.items(),
            key=lambda x: x[1],
            reverse=True
        )
    ]

    return {
        'window_id'          : window_key,
        'rolling_avg_per_min': rolling_avg,
        'borough_rates'      : borough_rates,
        'window_total'       : event_total
    }


def window3_event_count_and_surge(complaint_totals, borough_totals, event_total):
    """
    WINDOW 3 — Total event count and surge detection across full 5-minute window.
    All counts accumulated across the entire window — not just current batch.
    """
    window_key = get_window_key(WINDOW_MINUTES)

    # Surge alerts based on full window accumulated counts
    surge_alerts = [
        {
            'complaint_type': ct,
            'count'         : count,
            'threshold'     : SURGE_THRESHOLD,
            'severity'      : 'HIGH' if count >= SURGE_THRESHOLD * 2 else 'MEDIUM'
        }
        for ct, count in complaint_totals.items()
        if count >= SURGE_THRESHOLD
    ]

    top_borough = max(borough_totals.items(), key=lambda x: x[1]) \
                  if borough_totals else ('UNKNOWN', 0)

    borough_dominance = round(
        (top_borough[1] / event_total * 100), 2
    ) if event_total > 0 else 0

    return {
        'window_key'       : window_key,
        'total_events'     : event_total,
        'events_per_window': event_total,
        'surge_alerts'     : surge_alerts,
        'top_borough'      : top_borough[0],
        'borough_dominance': f"{borough_dominance}%",
        'window_size_mins' : WINDOW_MINUTES
    }


def write_results_to_dynamodb(top5, rolling, surge):
    """Write all three window results to DynamoDB and publish SNS if surges."""
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
            f"Rolling avg: {rolling['rolling_avg_per_min']}/min | "
            f"Total events: {surge['total_events']}"
        )

        # SNS alert when surge detected
        if surge['surge_alerts']:
            sns_client = boto3.client('sns', region_name='us-east-1')
            alert_lines = []
            for alert in surge['surge_alerts']:
                alert_lines.append(
                    f"  - {alert['complaint_type']}: "
                    f"{alert['count']} complaints "
                    f"[{alert['severity']} severity]"
                )

            message = (
                f"NYC 311 COMPLAINT SURGE ALERT\n"
                f"{'='*40}\n"
                f"Time Window:   {window_key}\n"
                f"Top Borough:   {surge['top_borough']} "
                f"({surge['borough_dominance']} of complaints)\n"
                f"Total Records: {int(surge['total_events'])} in this window\n\n"
                f"SURGES DETECTED:\n"
                f"{chr(10).join(alert_lines)}\n\n"
                f"{'='*40}\n"
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

    total_records                                   = len(records)
    complaint_counts, borough_counts, agency_counts = process_records(records)
    window_key                                      = get_window_key(WINDOW_MINUTES)

    # Step 1 — Accumulate current batch to DynamoDB
    accumulate_to_dynamodb(window_key, complaint_counts, borough_counts, total_records)

    # Step 2 — Read back FULL window totals from DynamoDB
    complaint_totals, borough_totals, event_total = read_window_totals(window_key)

    # Step 3 — Compute all three windows from full window data
    top5    = window1_top5_complaints(complaint_totals)
    rolling = window2_rolling_average(borough_totals, event_total)
    surge   = window3_event_count_and_surge(
        complaint_totals, borough_totals, event_total
    )

    # Step 4 — Write results and send SNS if needed
    write_results_to_dynamodb(top5, rolling, surge)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'records_processed': total_records,
            'window_key'       : window_key,
            'window_total'     : event_total,
            'top_complaint'    : top5[0]['complaint_type'] if top5 else 'N/A',
            'rolling_avg'      : rolling['rolling_avg_per_min'],
            'surge_alerts'     : len(surge['surge_alerts'])
        })
    }