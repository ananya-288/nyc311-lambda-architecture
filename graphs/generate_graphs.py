#!/usr/bin/env python3
"""
NYC 311 Performance Benchmarking Graphs
Phase 3 — Performance Measurement
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# Graph 1 — Speedup vs Worker Count
workers       = [1, 2, 3, 4]
job_times     = [319, 259, 208, 0]  # Run 4 pending — fill in after
speedups      = [1.0, 319/259, 319/208, 0]  # Run 4 pending

# Placeholder for Run 4 — update after it finishes
# job_times[3] = actual seconds
# speedups[3]  = 319 / actual seconds

fig1, ax1 = plt.subplots(figsize=(8, 5))
ax1.plot(workers[:3], speedups[:3], 'bo-', linewidth=2, markersize=8, label='Actual speedup')
ax1.plot(workers, workers, 'r--', linewidth=1, label='Ideal linear speedup')
ax1.set_xlabel('Number of EMR Worker Nodes', fontsize=12)
ax1.set_ylabel('Speedup (relative to 1 worker)', fontsize=12)
ax1.set_title('EMR Batch Job Speedup vs Worker Count\nNYC 311 Lambda Architecture', fontsize=13)
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.set_xticks(workers)
ax1.set_ylim(0, 5)
plt.tight_layout()
plt.savefig('speedup_graph.png', dpi=150, bbox_inches='tight')
print("Graph 1 saved: speedup_graph.png")

# Graph 2 — Throughput vs Ingestion Rate 
delays      = [0.1,  0.5,  1.0,  2.0]
rates       = [8.92, 1.95, 0.99, 0.50]

fig2, ax2 = plt.subplots(figsize=(8, 5))
ax2.plot(delays, rates, 'gs-', linewidth=2, markersize=8)
ax2.set_xlabel('Replay Delay (seconds per record)', fontsize=12)
ax2.set_ylabel('Throughput (records/second)', fontsize=12)
ax2.set_title('Kinesis Producer Throughput vs Replay Delay\nNYC 311 Lambda Architecture', fontsize=13)
ax2.grid(True, alpha=0.3)
for i, (d, r) in enumerate(zip(delays, rates)):
    ax2.annotate(f'{r} rec/s', (d, r), textcoords="offset points",
                xytext=(0, 10), ha='center', fontsize=10)
plt.tight_layout()
plt.savefig('throughput_graph.png', dpi=150, bbox_inches='tight')
print("Graph 2 saved: throughput_graph.png")

#  Graph 3 — EMR Job Duration vs Worker Count
fig3, ax3 = plt.subplots(figsize=(8, 5))
ax3.bar(workers[:3], job_times[:3], color=['#2196F3', '#4CAF50', '#FF9800'],
        width=0.5, edgecolor='black', linewidth=0.5)
ax3.set_xlabel('Number of EMR Worker Nodes', fontsize=12)
ax3.set_ylabel('Job Duration (seconds)', fontsize=12)
ax3.set_title('EMR Batch Job Duration vs Worker Count\nNYC 311 Lambda Architecture', fontsize=13)
ax3.set_xticks(workers[:3])
for i, v in enumerate(job_times[:3]):
    ax3.text(workers[i], v + 3, f'{v}s', ha='center', fontsize=11, fontweight='bold')
ax3.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig('duration_graph.png', dpi=150, bbox_inches='tight')
print("Graph 3 saved: duration_graph.png")

print("\nAll graphs generated!")
print("Update job_times[3] and speedups[3] after Run 4 completes")