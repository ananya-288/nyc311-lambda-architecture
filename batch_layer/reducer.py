#!/usr/bin/env python3
"""
NYC 311 Batch Layer — MapReduce Reducer
Aggregates complaint counts per type|borough|hour|day_of_week
Outputs historical baseline for serving layer comparison.
"""

import sys

current_key   = None
current_count = 0

for line in sys.stdin:
    try:
        line  = line.strip()
        parts = line.split('\t')

        if len(parts) != 2:
            continue

        key   = parts[0]
        count = int(parts[1])

        if key == current_key:
            current_count += count
        else:
            if current_key:
                # Output: complaint_type|borough|hour|day_of_week  count
                print(f"{current_key}\t{current_count}")
            current_key   = key
            current_count = count

    except Exception:
        pass

# Don't forget last key
if current_key:
    print(f"{current_key}\t{current_count}")