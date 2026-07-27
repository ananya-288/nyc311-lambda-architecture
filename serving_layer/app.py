#!/usr/bin/env python3
"""
NYC 311 Serving Layer — Flask Application
Combines speed layer (DynamoDB) and batch layer (S3/Athena)
to produce coherent merged dashboard.

This is the Lambda Architecture "merge" — combining:
  - Real-time top 5 surging complaints (from Lambda/DynamoDB)
  - Historical baselines (from EMR/S3)
  - Deviation analysis (anomaly detection)

"""

from flask import Flask, jsonify, render_template_string
import boto3
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

# Logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask app 
app = Flask(__name__)

# AWS clients 
dynamodb      = boto3.resource('dynamodb', region_name='us-east-1')
s3            = boto3.client('s3',         region_name='us-east-1')
results_table = dynamodb.Table('nyc311-speed-results')
counts_table  = dynamodb.Table('nyc311-complaint-counts')

BUCKET = 'nyc311-lambda-architecture'


# Helper 
def decimal_to_float(obj):
    """Convert DynamoDB Decimal to float for JSON serialisation."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def get_speed_layer_results():
    """
    Read latest speed layer results from DynamoDB.
    Returns top 5 complaints, rolling average, surge alerts.
    """
    try:
        # Scan for most recent window
        response = results_table.scan(Limit=10)
        items    = response.get('Items', [])

        if not items:
            return None

        # Sort by timestamp — get most recent
        items.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        latest = items[0]

        return {
            'window_id'        : latest.get('window_id', 'N/A'),
            'timestamp'        : latest.get('timestamp', 'N/A'),
            'top5_complaints'  : json.loads(latest.get('top5_complaints', '[]')),
            'rolling_avg'      : json.loads(latest.get('rolling_avg', '{}')),
            'total_records'    : int(latest.get('total_records', 0)),
            'surge_alerts'     : json.loads(latest.get('surge_alerts', '[]')),
            'top_borough'      : latest.get('top_borough', 'N/A'),
            'borough_dominance': latest.get('borough_dominance', '0%'),
        }

    except Exception as e:
        logger.error(f"Error reading speed layer: {e}")
        return None


def get_batch_baseline():
    """
    Read historical baseline from S3 batch results.
    Matches by complaint_type + borough + current hour + current day of week.
    """
    try:
        # Get current hour and day of week
        now         = datetime.now(timezone.utc)
        current_hour = now.hour
        current_dow  = now.isoweekday()  # 1=Monday, 7=Sunday

        baseline = {}
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix='batch-results/baseline/'
        )

        if 'Contents' not in response:
            logger.warning("No batch results found in S3")
            return {}

        # Read all part files
        for obj in response['Contents']:
            if not obj['Key'].endswith('.csv'):
                continue

            csv_obj = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
            content = csv_obj['Body'].read().decode('utf-8')
            lines   = content.strip().split('\n')

            for line in lines[1:]:  # skip header
                parts = line.split(',')
                if len(parts) < 5:
                    continue
                try:
                    complaint_type   = parts[0].strip()
                    borough          = parts[1].strip()
                    hour             = int(parts[2].strip())
                    day_of_week      = int(parts[3].strip())
                    historical_count = int(parts[4].strip())

                    # Only keep rows matching current hour and day
                    if hour == current_hour and day_of_week == current_dow:
                        key = f"{complaint_type}|{borough}"
                        baseline[key] = historical_count

                except (ValueError, IndexError):
                    continue

        logger.info(f"Loaded {len(baseline)} baseline entries for hour={current_hour} day={current_dow}")
        return baseline

    except Exception as e:
        logger.error(f"Error reading batch baseline: {e}")
        return {}


def merge_speed_and_batch(speed_results, batch_baseline):
    """
    THE LAMBDA MERGE — combines speed and batch results.
    Matches current complaints against historical baseline
    for same complaint_type + borough + hour + day_of_week.
    """
    if not speed_results:
        return []

    merged = []
    for complaint in speed_results.get('top5_complaints', []):
        complaint_type   = complaint['complaint_type']
        current_count    = complaint['count']

        # Try to match with borough from speed results
        top_borough      = speed_results.get('top_borough', 'UNKNOWN')
        key              = f"{complaint_type}|{top_borough}"
        historical_count = batch_baseline.get(key, 0)

        # Also try without borough match
        if historical_count == 0:
            for k, v in batch_baseline.items():
                if k.startswith(complaint_type):
                    historical_count = v
                    break

        # Compute deviation
        if historical_count > 0:
            deviation_pct = round(
                (current_count - historical_count) / historical_count * 100, 1
            )
            is_anomalous = abs(deviation_pct) > 50
        else:
            deviation_pct = None
            is_anomalous  = False

        merged.append({
            'rank'            : complaint['rank'],
            'complaint_type'  : complaint_type,
            'current_count'   : current_count,
            'historical_count': historical_count,
            'deviation_pct'   : deviation_pct,
            'is_anomalous'    : is_anomalous,
            'window'          : complaint.get('window', 'last_5_minutes'),
            'status'          : '⚠️ ANOMALY' if is_anomalous else '✓ Normal'
        })

    return merged


# Routes 

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now(timezone.utc).isoformat()})


@app.route('/api/speed')
def api_speed():
    """Raw speed layer results from DynamoDB."""
    results = get_speed_layer_results()
    if not results:
        return jsonify({'error': 'No speed layer results available'}), 404
    return jsonify(json.loads(json.dumps(results, default=decimal_to_float)))


@app.route('/api/batch')
def api_batch():
    """Raw batch baseline from S3."""
    baseline = get_batch_baseline()
    return jsonify({'baseline_complaint_types': len(baseline), 'sample': dict(list(baseline.items())[:5])})


@app.route('/api/merged')
def api_merged():
    """
    THE LAMBDA MERGE endpoint.
    Combines speed + batch results into one coherent view.
    """
    speed_results  = get_speed_layer_results()
    batch_baseline = get_batch_baseline()
    merged         = merge_speed_and_batch(speed_results, batch_baseline)

    return jsonify({
        'window_id'        : speed_results.get('window_id') if speed_results else 'N/A',
        'timestamp'        : datetime.now(timezone.utc).isoformat(),
        'top5_merged'      : merged,
        'rolling_avg'      : speed_results.get('rolling_avg') if speed_results else {},
        'surge_alerts'     : speed_results.get('surge_alerts') if speed_results else [],
        'top_borough'      : speed_results.get('top_borough') if speed_results else 'N/A',
        'borough_dominance': speed_results.get('borough_dominance') if speed_results else '0%',
        'total_records'    : speed_results.get('total_records') if speed_results else 0,
    })


@app.route('/')
def dashboard():
    """Main dashboard — shows merged batch + speed results."""
    speed_results  = get_speed_layer_results()
    batch_baseline = get_batch_baseline()
    merged         = merge_speed_and_batch(speed_results, batch_baseline)

    html = """
<!DOCTYPE html>
<html>
<head>
    <title>NYC 311 Complaint Surge Detection</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1   { color: #333; }
        .card { background: white; padding: 20px; margin: 10px 0;
                border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th    { background: #2c3e50; color: white; padding: 10px; text-align: left; }
        td    { padding: 10px; border-bottom: 1px solid #ddd; }
        .anomaly { background: #ffe6e6; color: #c0392b; font-weight: bold; }
        .normal  { background: #e6ffe6; color: #27ae60; }
        .metric  { font-size: 2em; font-weight: bold; color: #2c3e50; }
        .label   { color: #7f8c8d; font-size: 0.9em; }
    </style>
</head>
<body>
    <h1>🗽 NYC 311 Real-Time Complaint Surge Detection</h1>
    <p><b>Lambda Architecture</b> — Speed Layer (DynamoDB) + Batch Layer (S3) Merged View</p>
    <p>Auto-refreshes every 30 seconds | 
       Window: {{ window_id }} | 
       Updated: {{ timestamp }}</p>

    <!-- Speed Layer Summary -->
    <div class="card">
        <h2>⚡ Speed Layer — Live Metrics (Last 5 Minutes)</h2>
        <table>
            <tr>
                <td>
                    <div class="metric">{{ total_records }}</div>
                    <div class="label">Records in window</div>
                </td>
                <td>
                    <div class="metric">{{ rolling_avg }}</div>
                    <div class="label">Rolling avg (per min)</div>
                </td>
                <td>
                    <div class="metric">{{ top_borough }}</div>
                    <div class="label">Top borough ({{ borough_dominance }})</div>
                </td>
                <td>
                    <div class="metric">{{ surge_count }}</div>
                    <div class="label">Active surge alerts</div>
                </td>
            </tr>
        </table>
    </div>

    <!-- Merged View — Lambda Merge -->
    <div class="card">
        <h2>🔀 Lambda Merge — Current vs Historical Baseline</h2>
        <p>Combining real-time speed layer with EMR batch baseline</p>
        <table>
            <tr>
                <th>Rank</th>
                <th>Complaint Type</th>
                <th>Now (5 min)</th>
                <th>Historical Avg</th>
                <th>Deviation</th>
                <th>Status</th>
            </tr>
            {% for item in merged %}
            <tr class="{{ 'anomaly' if item.is_anomalous else 'normal' }}">
                <td>{{ item.rank }}</td>
                <td>{{ item.complaint_type }}</td>
                <td>{{ item.current_count }}</td>
                <td>{{ item.historical_count }}</td>
                <td>{{ item.deviation_pct }}%</td>
                <td>{{ item.status }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>

    <!-- Surge Alerts -->
    {% if surge_alerts %}
    <div class="card">
        <h2>🚨 Active Surge Alerts</h2>
        {% for alert in surge_alerts %}
        <p class="anomaly">
            {{ alert.complaint_type }} — 
            {{ alert.count }} complaints 
            ({{ alert.severity }} severity)
        </p>
        {% endfor %}
    </div>
    {% endif %}

</body>
</html>
"""

    # Prepare template variables
    rolling_avg   = 'N/A'
    window_id     = 'N/A'
    total_records = 0
    top_borough   = 'N/A'
    borough_dom   = '0%'
    surge_alerts  = []
    surge_count   = 0

    if speed_results:
        window_id     = speed_results.get('window_id', 'N/A')
        total_records = speed_results.get('total_records', 0)
        top_borough   = speed_results.get('top_borough', 'N/A')
        borough_dom   = speed_results.get('borough_dominance', '0%')
        surge_alerts  = speed_results.get('surge_alerts', [])
        surge_count   = len(surge_alerts)
        rolling       = speed_results.get('rolling_avg', {})
        rolling_avg   = rolling.get('rolling_avg_per_min', 'N/A') if rolling else 'N/A'

    from flask import render_template_string
    return render_template_string(
        html,
        merged           = merged,
        window_id        = window_id,
        timestamp        = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        total_records    = total_records,
        rolling_avg      = rolling_avg,
        top_borough      = top_borough,
        borough_dominance= borough_dom,
        surge_alerts     = surge_alerts,
        surge_count      = surge_count
    )


if __name__ == '__main__':
    logger.info("Starting NYC 311 Serving Layer...")
    logger.info("Dashboard: http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)