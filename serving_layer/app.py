#!/usr/bin/env python3
"""
NYC 311 Serving Layer — Flask Application
Combines speed layer (DynamoDB) and batch layer (S3/Athena)
"""

from flask import Flask, jsonify, render_template, request, Response
import boto3
import json
import csv
import io
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

dynamodb      = boto3.resource('dynamodb', region_name='us-east-1')
s3            = boto3.client('s3',         region_name='us-east-1')
results_table = dynamodb.Table('nyc311-speed-results')

BUCKET = 'nyc311-lambda-architecture'


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def get_speed_layer_results():
    try:
        response = results_table.scan(Limit=10)
        items    = response.get('Items', [])
        if not items:
            return None
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
    try:
        athena       = boto3.client('athena', region_name='us-east-1')
        now          = datetime.now(timezone.utc)
        current_hour = now.hour
        current_dow  = now.isoweekday() % 7 + 1

        query = f"""
            SELECT complaint_type, borough, avg_count_per_5min_window
            FROM nyc311_db.nyc311_baseline
            WHERE hour = {current_hour}
            AND day_of_week = {current_dow}
        """

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'nyc311_db'},
            ResultConfiguration={'OutputLocation': 's3://nyc311-athena-results/'}
        )
        query_id = response['QueryExecutionId']

        for _ in range(30):
            status = athena.get_query_execution(
                QueryExecutionId=query_id
            )['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                break
            elif status in ('FAILED', 'CANCELLED'):
                return {}
            time.sleep(1)

        results  = athena.get_query_results(QueryExecutionId=query_id)
        baseline = {}
        for row in results['ResultSet']['Rows'][1:]:
            values = [col.get('VarCharValue', '') for col in row['Data']]
            if len(values) >= 3:
                try:
                    key = (values[0], values[1], current_hour, current_dow)
                    baseline[key] = float(values[2])
                except (ValueError, IndexError):
                    continue

        logger.info(f"Athena returned {len(baseline)} baseline entries")
        return baseline
    except Exception as e:
        logger.error(f"Athena error: {e}")
        return {}


def get_batch_top5():
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix='batch-results/top5-overall/')
        if 'Contents' not in response:
            return []
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                content = s3.get_object(Bucket=BUCKET, Key=obj['Key'])['Body'].read().decode('utf-8')
                lines   = content.strip().split('\n')
                result  = []
                for line in lines[1:]:
                    parts = line.split(',')
                    if len(parts) >= 2:
                        try:
                            result.append({
                                'complaint_type': parts[0].strip(),
                                'total_count'   : int(parts[1].strip())
                            })
                        except (ValueError, IndexError):
                            continue
                return result[:5]
        return []
    except Exception as e:
        logger.error(f"Error reading batch top5: {e}")
        return []


def get_batch_borough_summary():
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix='batch-results/borough-summary/')
        if 'Contents' not in response:
            return []
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                content = s3.get_object(Bucket=BUCKET, Key=obj['Key'])['Body'].read().decode('utf-8')
                lines   = content.strip().split('\n')
                result  = []
                for line in lines[1:]:
                    parts = line.split(',')
                    if len(parts) >= 2:
                        try:
                            result.append({
                                'borough'         : parts[0].strip(),
                                'total_complaints': int(parts[1].strip()),
                            })
                        except (ValueError, IndexError):
                            continue
                return result
        return []
    except Exception as e:
        logger.error(f"Error reading borough summary: {e}")
        return []


def merge_speed_and_batch(speed_results, batch_baseline):
    if not speed_results:
        return []

    now          = datetime.now(timezone.utc)
    current_hour = now.hour
    current_dow  = now.isoweekday() % 7 + 1
    top_borough  = speed_results.get('top_borough', 'UNKNOWN')

    merged = []
    for complaint in speed_results.get('top5_complaints', []):
        complaint_type = complaint['complaint_type']
        current_count  = complaint['count']

        key            = (complaint_type, top_borough, current_hour, current_dow)
        historical_avg = batch_baseline.get(key, 0)

        if historical_avg == 0:
            for k, v in batch_baseline.items():
                if k[0] == complaint_type and k[2] == current_hour and k[3] == current_dow:
                    historical_avg = v
                    break

        if historical_avg > 0:
            deviation_pct = round(
                (current_count - historical_avg) / historical_avg * 100, 1
            )
            is_anomalous = abs(deviation_pct) > 50
        else:
            deviation_pct = None
            is_anomalous  = False

        merged.append({
            'rank'          : complaint['rank'],
            'complaint_type': complaint_type,
            'current_count' : current_count,
            'historical_avg': round(historical_avg, 2),
            'deviation_pct' : deviation_pct,
            'is_anomalous'  : is_anomalous,
            'window'        : complaint.get('window', 'last_5_minutes'),
            'status'        : 'ANOMALY' if is_anomalous else 'Normal'
        })

    return merged


# API Routes 

@app.route('/health')
def health():
    return jsonify({
        'status'   : 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/api/speed')
def api_speed():
    results = get_speed_layer_results()
    if not results:
        return jsonify({'error': 'No speed layer results'}), 404
    return jsonify(json.loads(json.dumps(results, default=decimal_to_float)))


@app.route('/api/batch')
def api_batch():
    return jsonify({
        'top5_alltime'   : get_batch_top5(),
        'borough_summary': get_batch_borough_summary()
    })


@app.route('/api/merged')
def api_merged():
    speed_results  = get_speed_layer_results()
    batch_baseline = get_batch_baseline()
    merged         = merge_speed_and_batch(speed_results, batch_baseline)

    anomalous_only = request.args.get('anomalous', 'false') == 'true'
    severity       = request.args.get('severity', 'all')

    surge_alerts = speed_results.get('surge_alerts', []) if speed_results else []

    if severity != 'all':
        surge_alerts = [a for a in surge_alerts
                        if a.get('severity', '').upper() == severity.upper()]
    if anomalous_only:
        merged = [m for m in merged if m['is_anomalous']]

    return jsonify({
        'window_id'        : speed_results.get('window_id') if speed_results else 'N/A',
        'timestamp'        : datetime.now(timezone.utc).isoformat(),
        'top5_merged'      : merged,
        'rolling_avg'      : speed_results.get('rolling_avg') if speed_results else {},
        'surge_alerts'     : surge_alerts,
        'top_borough'      : speed_results.get('top_borough') if speed_results else 'N/A',
        'borough_dominance': speed_results.get('borough_dominance') if speed_results else '0%',
        'total_records'    : speed_results.get('total_records') if speed_results else 0,
    })


@app.route('/api/export/merged')
def export_merged():
    speed_results  = get_speed_layer_results()
    batch_baseline = get_batch_baseline()
    merged         = merge_speed_and_batch(speed_results, batch_baseline)
    anomalous_only = request.args.get('anomalous', 'false') == 'true'
    if anomalous_only:
        merged = [m for m in merged if m['is_anomalous']]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        'rank', 'complaint_type', 'current_count',
        'historical_avg', 'deviation_pct', 'is_anomalous', 'status'
    ])
    writer.writeheader()
    writer.writerows(merged)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=nyc311_merged_{ts}.csv'}
    )


@app.route('/api/export/alerts')
def export_alerts():
    speed_results = get_speed_layer_results()
    surge_alerts  = speed_results.get('surge_alerts', []) if speed_results else []
    severity      = request.args.get('severity', 'all')
    if severity != 'all':
        surge_alerts = [a for a in surge_alerts
                        if a.get('severity', '').upper() == severity.upper()]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        'complaint_type', 'count', 'severity', 'threshold'
    ])
    writer.writeheader()
    writer.writerows(surge_alerts)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=nyc311_alerts_{ts}.csv'}
    )


@app.route('/api/export/baseline')
def export_baseline():
    batch_baseline = get_batch_baseline()
    output         = io.StringIO()
    writer         = csv.writer(output)
    writer.writerow([
        'complaint_type', 'borough', 'hour',
        'day_of_week', 'avg_count_per_5min_window'
    ])
    for (ct, borough, hour, dow), avg in batch_baseline.items():
        writer.writerow([ct, borough, hour, dow, avg])

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=nyc311_baseline_{ts}.csv'}
    )


@app.route('/')
def dashboard():
    speed_results  = get_speed_layer_results()
    batch_baseline = get_batch_baseline()
    merged         = merge_speed_and_batch(speed_results, batch_baseline)
    batch_top5     = get_batch_top5()
    batch_boroughs = get_batch_borough_summary()

    rolling_avg   = 'N/A'
    window_id     = 'N/A'
    total_records = 0
    top_borough   = 'N/A'
    borough_dom   = '0%'
    surge_alerts  = []
    surge_count   = 0
    borough_rates = []
    max_rate      = 1

    if speed_results:
        window_id     = speed_results.get('window_id', 'N/A')
        total_records = speed_results.get('total_records', 0)
        top_borough   = speed_results.get('top_borough', 'N/A')
        borough_dom   = speed_results.get('borough_dominance', '0%')
        surge_alerts  = speed_results.get('surge_alerts', [])
        surge_count   = len(surge_alerts)
        rolling       = speed_results.get('rolling_avg', {})
        if rolling:
            rolling_avg   = rolling.get('rolling_avg_per_min', 'N/A')
            borough_rates = rolling.get('borough_rates', [])
            if borough_rates:
                max_rate = max(b.get('projected_rate', 1) for b in borough_rates) or 1

    return render_template(
        'dashboard.html',
        merged            = merged,
        window_id         = window_id,
        timestamp         = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        total_records     = total_records,
        rolling_avg       = rolling_avg,
        top_borough       = top_borough,
        borough_dominance = borough_dom,
        surge_alerts      = surge_alerts,
        surge_count       = surge_count,
        borough_rates     = borough_rates,
        max_rate          = max_rate,
        batch_top5        = batch_top5,
        batch_boroughs    = batch_boroughs,
    )


if __name__ == '__main__':
    logger.info("Starting NYC 311 Serving Layer...")
    app.run(host='0.0.0.0', port=5000, debug=False)