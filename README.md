# NYC 311 Complaint Surge Detection - Lambda Architecture on AWS

## Core Question
What are the top 5 fastest-surging complaint types in NYC right now 
and are these surges historically anomalous for this time of day, 
day of week, and borough?

## Dataset
- Source: NYC Open Data — 311 Service Requests 2020 to Present
- Rows: 7,350,000 | Columns: 19 | Size: 3.40 GiB
- License: Public Domain 

## Architecture
Kinesis → Lambda + Firehose → DynamoDB + EMR → Flask on EC2

## Team
Ananya & Satvik - NCI MSc Cloud Computing 2026