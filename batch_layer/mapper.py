#!/usr/bin/env python3
"""
NYC 311 Batch Layer — MapReduce Mapper
Reads NYC 311 complaints and emits:
key: complaint_type|borough|hour|day_of_week
value: 1

Includes custom partitioner and
Partitions output by borough for balanced distribution.

"""

import sys
import csv
from datetime import datetime


def partitioner(key, num_reducers=5):
    """
    Partitions MapReduce output by borough
    for balanced distribution across reducers.
    """
    parts  = key.split('|')
    borough = parts[1] if len(parts) > 1 else 'UNKNOWN'

    borough_map = {
        'MANHATTAN'    : 0,
        'BROOKLYN'     : 1,
        'QUEENS'       : 2,
        'BRONX'        : 3,
        'STATEN ISLAND': 4
    }
    return borough_map.get(borough, 0) % num_reducers


for line in sys.stdin:
    try:
        # Skip header
        if line.startswith('unique_key'):
            continue

        # Use csv reader to handle embedded commas in text fields
        reader     = csv.reader([line.strip()])
        fields     = next(reader)

        if len(fields) < 19:
            continue

        # Extract fields by correct index
        # Column order: unique_key(0), created_date(1), closed_date(2),
        # agency(3), agency_name(4), complaint_type(5), descriptor(6),
        # location_type(7), incident_zip(8), incident_address(9),
        # borough(10), status(11), resolution_description(12),
        # community_board(13), council_district(14), police_precinct(15),
        # latitude(16), longitude(17), open_data_channel_type(18)

        complaint_type = fields[5].strip().strip('"')
        borough        = fields[10].strip().strip('"')  # Fixed: was 15
        created_date   = fields[1].strip().strip('"')

        # Skip empty or unknown
        if not complaint_type or not borough:
            continue
        if complaint_type in ('nan', '') or borough in ('nan', ''):
            continue

        # Parse hour and day of week
        dt          = datetime.strptime(created_date[:19], '%Y-%m-%dT%H:%M:%S')
        hour        = dt.hour
        day         = dt.weekday()  # 0=Monday, 6=Sunday

        # Emit key-value pair
        key = f"{complaint_type}|{borough}|{hour}|{day}"

        # Apply custom partitioner
        partition = partitioner(key)

        # Output: partition_key TAB key TAB value
        print(f"{partition}\t{key}\t1")

    except Exception:
        pass