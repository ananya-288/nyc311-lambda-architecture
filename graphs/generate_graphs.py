#!/usr/bin/env python3
"""
NYC 311 Performance Benchmarking Graphs
Phase 3 — Performance Measurement
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# Data

workers = [1, 2, 3, 4]

# Compute-only times (Ready → End) in seconds
compute_times = [319, 259, 207, 190]

# Total wall-clock times (Created → End) in seconds
total_times   = [579, 516, 465, 448]

# Speedup ratios
compute_speedup = [compute_times[0] / t for t in compute_times]
total_speedup   = [total_times[0]   / t for t in total_times]

# Producer throughput data
delays = [0.1,  0.5,  1.0,  2.0]
rates  = [8.92, 1.95, 0.99, 0.50]

# Graph 1 — Speedup vs Worker Count 
fig1, ax1 = plt.subplots(figsize=(9, 5))

ax1.plot(workers, compute_speedup, 'bo-',
         linewidth=2, markersize=8,
         label='Compute-only speedup (Ready→End)')
ax1.plot(workers, total_speedup, 'gs-',
         linewidth=2, markersize=8,
         label='Total wall-clock speedup (Created→End)')
ax1.plot(workers, workers, 'r--',
         linewidth=1, label='Ideal linear speedup')

# Annotate compute speedup values
for i, (w, s) in enumerate(zip(workers, compute_speedup)):
    ax1.annotate(f'{s:.2f}×',
                xy=(w, s),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center', fontsize=10)

ax1.set_xlabel('Number of EMR Worker Nodes', fontsize=12)
ax1.set_ylabel('Speedup (relative to 1 worker)', fontsize=12)
ax1.set_title(
    'EMR Batch Job Speedup vs Worker Count\n'
    'NYC 311 Lambda Architecture — Bootstrap overhead excluded from compute speedup',
    fontsize=12
)
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.set_xticks(workers)
ax1.set_ylim(0, 5)
plt.tight_layout()
plt.savefig('speedup_graph.png', dpi=150, bbox_inches='tight')
print("Graph 1 saved: speedup_graph.png")
plt.close()

# Graph 2 — Job Duration vs Worker Count 
fig2, ax2 = plt.subplots(figsize=(9, 5))

x     = np.arange(len(workers))
width = 0.35

bars1 = ax2.bar(x - width/2, compute_times, width,
                label='Compute-only (Ready→End)',
                color='#2196F3', edgecolor='black', linewidth=0.5)
bars2 = ax2.bar(x + width/2, total_times, width,
                label='Total wall-clock (Created→End)',
                color='#90CAF9', edgecolor='black', linewidth=0.5)

# Add value labels
for bar in bars1:
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
             f'{int(bar.get_height())}s',
             ha='center', fontsize=9, fontweight='bold')
for bar in bars2:
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
             f'{int(bar.get_height())}s',
             ha='center', fontsize=9)

ax2.set_xlabel('Number of EMR Worker Nodes', fontsize=12)
ax2.set_ylabel('Job Duration (seconds)', fontsize=12)
ax2.set_title(
    'EMR Batch Job Duration vs Worker Count\n'
    'NYC 311 Lambda Architecture',
    fontsize=12
)
ax2.set_xticks(x)
ax2.set_xticklabels([f'{w} worker{"s" if w > 1 else ""}' for w in workers])
ax2.legend(fontsize=10)
ax2.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig('duration_graph.png', dpi=150, bbox_inches='tight')
print("Graph 2 saved: duration_graph.png")
plt.close()

# Graph 3 — Throughput vs Replay Delay 
fig3, ax3 = plt.subplots(figsize=(9, 5))

ax3.plot(delays, rates, 'gs-', linewidth=2, markersize=8)
ax3.fill_between(delays, rates, alpha=0.1, color='green')

for d, r in zip(delays, rates):
    ax3.annotate(f'{r} rec/s',
                xy=(d, r),
                xytext=(0, 12),
                textcoords='offset points',
                ha='center', fontsize=10)

ax3.set_xlabel('Replay Delay (seconds per record)', fontsize=12)
ax3.set_ylabel('Throughput (records/second)', fontsize=12)
ax3.set_title(
    'Kinesis Producer Throughput vs Replay Delay\n'
    'NYC 311 Lambda Architecture',
    fontsize=12
)
ax3.grid(True, alpha=0.3)
ax3.set_xlim(-0.1, 2.2)
plt.tight_layout()
plt.savefig('throughput_graph.png', dpi=150, bbox_inches='tight')
print("Graph 3 saved: throughput_graph.png")
plt.close()

# Graph 4 — Kinesis Throughput over Time 
import matplotlib.dates as mdates
from datetime import datetime, timezone

# Real CloudWatch data
times = [
    datetime(2026, 7, 27, 14, 15, tzinfo=timezone.utc),
    datetime(2026, 7, 27, 14, 16, tzinfo=timezone.utc),
    datetime(2026, 7, 27, 14, 17, tzinfo=timezone.utc),
    datetime(2026, 7, 27, 14, 18, tzinfo=timezone.utc),
]
records = [66, 134, 43, 157]

fig4, ax4 = plt.subplots(figsize=(9, 5))
ax4.bar(range(len(times)), records,
        color='#2196F3', edgecolor='black', linewidth=0.5)
ax4.set_xticks(range(len(times)))
ax4.set_xticklabels(['14:15', '14:16', '14:17', '14:18'])

for i, v in enumerate(records):
    ax4.text(i, v + 2, str(int(v)), ha='center', fontsize=11, fontweight='bold')

ax4.set_xlabel('Time (UTC)', fontsize=12)
ax4.set_ylabel('Records Ingested per Minute', fontsize=12)
ax4.set_title(
    'Kinesis Stream Throughput over Time\n'
    'NYC 311 Lambda Architecture — Real CloudWatch Metrics',
    fontsize=12
)
ax4.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig('kinesis_throughput_graph.png', dpi=150, bbox_inches='tight')
print("GRaph 4 saved: kinesis_throughput_graph.png")
plt.close()

# Graph 5 — Lambda Latency vs Ingestion Rate 
fig5, ax5 = plt.subplots(figsize=(9, 5))

rates   = [0.50, 0.99, 1.95, 8.92]
latency = [61,   60,   150,  144]

ax5.plot(rates, latency, 'bo-', linewidth=2, markersize=8)

for r, l in zip(rates, latency):
    ax5.annotate(f'{l}ms',
                xy=(r, l),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center', fontsize=10)

ax5.set_xlabel('Ingestion Rate (records/second)', fontsize=12)
ax5.set_ylabel('Lambda Duration (ms)', fontsize=12)
ax5.set_title(
    'Lambda Latency vs Ingestion Rate\n'
    'NYC 311 Lambda Architecture',
    fontsize=12
)
ax5.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('latency_graph.png', dpi=150, bbox_inches='tight')
print("Graph 5 saved: latency_graph.png")
plt.close()

# Print summary
print("\n BENCHMARK SUMMARY")
print("=" * 55)
print(f"{'Workers':<10} {'Compute(s)':<14} {'Total(s)':<12} {'Compute Speedup':<18} {'Total Speedup'}")
print("-" * 55)
for i, w in enumerate(workers):
    print(f"{w:<10} {compute_times[i]:<14} {total_times[i]:<12} {compute_speedup[i]:<18.2f} {total_speedup[i]:.2f}")

print("\nAll graphs generated successfully!")