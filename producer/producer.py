#!/usr/bin/env python3
"""
NYC 311 Complaint Stream Producer
Reads NYC 311 dataset from S3 and replays it into Kinesis Data Streams at a controlled rate to simulate a live stream.
"""

import boto3
import pandas as pd
import json
import time
import logging
import argparse
from datetime import datetime
from io import StringIO

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
STREAM_NAME    = 'nyc311-complaint-stream'
BUCKET_NAME    = 'nyc311-lambda-architecture'
S3_KEY         = 'raw-data/nyc311_clean.csv'
REGION         = 'us-east-1'
CHUNK_SIZE     = 1000   # rows read from S3 at a time
DEFAULT_DELAY  = 1.0    # seconds between records (replay speed)

# AWS clients
kinesis = boto3.client('kinesis', region_name=REGION)
s3      = boto3.client('s3',      region_name=REGION)


def read_s3_chunks(bucket, key, chunksize):
    """Stream the CSV from S3 in chunks to avoid loading 3.4 GiB into memory."""
    logger.info(f"Connecting to s3://{bucket}/{key}")
    
    response = s3.get_object(Bucket=bucket, Key=key)
    
    chunks = pd.read_csv(
        response['Body'],
        chunksize=chunksize,
        low_memory=False,
        on_bad_lines='skip'
    )
    
    return chunks


def build_record(row):
    
    """ Convert a DataFrame row into a clean JSON record for Kinesis and include fields relevant to surge detection. """
    return {
        'unique_key'            : str(row.get('unique_key',    '')),
        'created_date'          : str(row.get('created_date',  '')),
        'closed_date'           : str(row.get('closed_date',   '')),
        'agency'                : str(row.get('agency',        '')),
        'agency_name'           : str(row.get('agency_name',   '')),
        'complaint_type'        : str(row.get('complaint_type','UNKNOWN')),
        'descriptor'            : str(row.get('descriptor',    '')),
        'location_type'         : str(row.get('location_type', '')),
        'incident_zip'          : str(row.get('incident_zip',  '')),
        'incident_address'      : str(row.get('incident_address','')),
        'borough'               : str(row.get('borough',       'UNKNOWN')),
        'status'                : str(row.get('status',        '')),
        'community_board'       : str(row.get('community_board','')),
        'council_district'      : str(row.get('council_district','')),
        'police_precinct'       : str(row.get('police_precinct','')),
        'latitude'              : str(row.get('latitude',      '')),
        'longitude'             : str(row.get('longitude',     '')),
        'open_data_channel_type': str(row.get('open_data_channel_type','')),
        'ingestion_timestamp'   : datetime.utcnow().isoformat() + 'Z',
    }


def send_to_kinesis(record, stream_name):
    """Send a single record to Kinesis."""
    kinesis.put_record(
        StreamName   = stream_name,
        Data         = json.dumps(record).encode('utf-8'),
        PartitionKey = record.get('borough', 'UNKNOWN'),
    )


def run_producer(delay, max_records, stream_name):
    """
    Main loop — replay dataset into Kinesis at controlled rate.
    """
    logger.info("NYC 311 COMPLAINT STREAM PRODUCER")
    logger.info(f"  Stream      : {stream_name}")
    logger.info(f"  Replay delay: {delay}s per record")
    logger.info(f"  Max records : {max_records if max_records else 'unlimited'}")

    total_sent   = 0
    total_errors = 0
    start_time   = time.time()

    try:
        chunks = read_s3_chunks(BUCKET_NAME, S3_KEY, CHUNK_SIZE)

        for chunk in chunks:
            for _, row in chunk.iterrows():

                # Stop if max_records reached
                if max_records and total_sent >= max_records:
                    logger.info(f"Reached max_records limit ({max_records}).")
                    return

                try:
                    record = build_record(row)

                    # Skip records with unknown complaint type or borough
                    if record['complaint_type'] in ('UNKNOWN', 'nan', ''):
                        continue
                    if record['borough'] in ('UNKNOWN', 'nan', ''):
                        continue

                    send_to_kinesis(record, stream_name)
                    total_sent += 1

                    # Progress every 100 records
                    if total_sent % 100 == 0:
                        elapsed   = time.time() - start_time
                        rate      = total_sent / elapsed
                        logger.info(
                            f"Sent: {total_sent:,} records | "
                            f"Errors: {total_errors} | "
                            f"Rate: {rate:.1f} rec/s | "
                            f"Elapsed: {elapsed:.0f}s"
                        )

                    time.sleep(delay)

                except Exception as e:
                    total_errors += 1
                    logger.warning(f"Error on record {total_sent}: {e}")
                    time.sleep(1)  # brief pause before retrying

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")

    finally:
        elapsed = time.time() - start_time
        logger.info("PRODUCER SUMMARY")
        logger.info(f"  Total sent  : {total_sent:,}")
        logger.info(f"  Total errors: {total_errors}")
        logger.info(f"  Elapsed     : {elapsed:.0f}s")
        logger.info(f"  Avg rate    : {total_sent/elapsed:.2f} rec/s")


# Entry point
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='NYC 311 Kinesis Producer — replay dataset as live stream'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=DEFAULT_DELAY,
        help='Seconds between records (default: 1.0). '
             'Use 0.1 for benchmarking, 3.0 for demo.'
    )
    parser.add_argument(
        '--max-records',
        type=int,
        default=0,
        help='Stop after N records (default: 0 = unlimited)'
    )
    parser.add_argument(
        '--stream',
        type=str,
        default=STREAM_NAME,
        help=f'Kinesis stream name (default: {STREAM_NAME})'
    )
    args = parser.parse_args()

    run_producer(
        delay       = args.delay,
        max_records = args.max_records,
        stream_name = args.stream,
    )