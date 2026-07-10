#!/usr/bin/env python3
"""
NYC 311 Speed Layer — Lambda Function
Triggered by Kinesis Data Stream records.

Implements THREE sliding windows (H1 discriminator):
  Window 1: Top 5 surging complaint types (last 10 minutes)
  Window 2: Borough-level surge detection (last 10 minutes)
  Window 3: Complaint index transition detection
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

# ── AWS clients ─────────────────────────────────────────────────
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
counts_table  = dynamodb.Table('nyc311-complaint-counts')
results_table = dynamodb.Table('nyc311-speed-results')

# ── Window configuration ────────────────────────────────────────
WINDOW_MINUTES = 10   # sliding window size
TOP_N          = 5    # top N complaint types to track


def decode_record(record):
    """Decode a base64-encoded Kinesis record into a dict."""
    raw  = base64.b64decode(record['kinesis']['data']).decode('utf-8')
    return json.loads(raw)


def get_window_key():
    """
    Returns current 10-minute window key.
    e.g. '2026-07-05T21:10' for any time between 21:10 and 21:19
    """
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute = (now.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second = 0,
        microsecond = 0
    )
    return floored.strftime('%Y-%m-%dT%H:%M')


def get_previous_window_key():
    """Returns the previous 10-minute window key for trend comparison."""
    now     = datetime.now(timezone.utc)
    floored = now.replace(
        minute = (now.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second = 0,
        microsecond = 0
    )
    prev = floored - timedelta(minutes=WINDOW_MINUTES)
    return prev.strftime('%Y-%m-%dT%H:%M')


def update_complaint_counts(records):
    """
    Window 1 — Update rolling complaint type counts.
    Increments count for each complaint_type in current window.
    """
    window_key = get_window_key()
    
    # Count occurrences in this batch
    batch_counts = defaultdict(int)
    borough_counts = defaultdict(int)
    
    for record in records:
        complaint_type = record.get('complaint_type', 'UNKNOWN')
        borough        = record.get('borough', 'UNKNOWN')
        
        if complaint_type not in ('UNKNOWN', 'nan', ''):
            batch_counts[complaint_type] += 1
        if borough not in ('UNKNOWN', 'nan', ''):
            borough_counts[borough] += 1
    
    # Update DynamoDB atomically for each complaint type
    for complaint_type, count in batch_counts.items():
        try:
            counts_table.update_item(
                Key={'complaint_type': complaint_type},
                UpdateExpression='''
                    SET window_key = :wk,
                        #cnt = if_not_exists(#cnt, :zero) + :inc,
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
            logger.error(f"Error updating count for {complaint_type}: {e}")
    
    return batch_counts, borough_counts


def compute_top5_and_write_results(batch_counts, borough_counts):
    """
    Window 2 — Compute top 5 surging complaint types.
    Window 3 — Detect borough surge.
    Write combined results to nyc311-speed-results table.
    """
    window_key      = get_window_key()
    prev_window_key = get_previous_window_key()
    
    # Sort complaint types by count descending
    top5 = sorted(
        batch_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:TOP_N]
    
    # Sort boroughs by count descending
    top_boroughs = sorted(
        borough_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    
    # Build top 5 list
    top5_list = [
        {
            'rank'          : i + 1,
            'complaint_type': ct,
            'count'         : int(count),
        }
        for i, (ct, count) in enumerate(top5)
    ]
    
    # Build borough surge list
    borough_list = [
        {
            'rank'   : i + 1,
            'borough': b,
            'count'  : int(count),
        }
        for i, (b, count) in enumerate(top_boroughs)
    ]
    
    # Window 3 — Index transition
    # Flag if any complaint type count exceeds threshold
    SURGE_THRESHOLD = 10  # records per window = anomaly
    surge_alerts = [
        ct for ct, count in batch_counts.items()
        if count >= SURGE_THRESHOLD
    ]
    
    # Write to results table
    try:
        results_table.put_item(
            Item={
                'window_id'      : window_key,
                'top5_complaints': json.dumps(top5_list),
                'top_boroughs'   : json.dumps(borough_list),
                'surge_alerts'   : json.dumps(surge_alerts),
                'total_records'  : int(sum(batch_counts.values())),
                'timestamp'      : datetime.now(timezone.utc).isoformat(),
                'prev_window_id' : prev_window_key,
            }
        )
        logger.info(f"Window {window_key}: top5={top5_list[:2]}...")
        
    except Exception as e:
        logger.error(f"Error writing results: {e}")


def lambda_handler(event, context):
    """
    Main Lambda handler — triggered by Kinesis stream.
    Processes each batch of records.
    """
    logger.info(f"Processing {len(event['Records'])} Kinesis records")
    
    # ── Decode all records ───────────────────────────────────────
    records = []
    for kinesis_record in event['Records']:
        try:
            record = decode_record(kinesis_record)
            records.append(record)
        except Exception as e:
            logger.warning(f"Could not decode record: {e}")
    
    if not records:
        logger.warning("No valid records to process")
        return {'statusCode': 200, 'body': 'No valid records'}
    
    logger.info(f"Decoded {len(records)} valid records")
    
    # ── Window 1 + 2: Update counts ──────────────────────────────
    batch_counts, borough_counts = update_complaint_counts(records)
    
    # ── Window 2 + 3: Compute top 5 and write results ────────────
    compute_top5_and_write_results(batch_counts, borough_counts)
    
    # ── Log summary ──────────────────────────────────────────────
    logger.info(
        f"Processed {len(records)} records | "
        f"Complaint types: {len(batch_counts)} | "
        f"Boroughs: {len(borough_counts)}"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'records_processed': len(records),
            'complaint_types'  : len(batch_counts),
            'window_key'       : get_window_key()
        })
    }