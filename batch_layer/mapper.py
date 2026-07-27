#!/usr/bin/env python3
"""
NYC 311 Batch Layer - MapReduce Mapper
"""

import sys
from datetime import datetime

for line in sys.stdin:
    try:
        # Skip header
        if line.startswith('unique_key'):
            continue

        fields = line.strip().split(',')

        # Extract fields by index
        complaint_type = fields[5].strip().strip('"')
        borough = fields[10].strip().strip('"')
        created_date   = fields[1].strip().strip('"')

        # Skip empty or unknown
        if not complaint_type or not borough:
            continue
        if complaint_type in ('nan', '') or borough in ('nan', ''):
            continue

        # Parse hour and day of week
        dt       = datetime.strptime(created_date[:19], '%Y-%m-%dT%H:%M:%S')
        hour     = dt.hour
        day      = dt.weekday()  # 0=Monday, 6=Sunday

        # Emit key-value pair
        key = f"{complaint_type}|{borough}|{hour}|{day}"
        print(f"{key}\t1")

    except Exception:
        pass