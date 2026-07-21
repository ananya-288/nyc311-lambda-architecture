# NYC 311 Complaint Surge Detection - Lambda Architecture on AWS

## Core Question
Which NYC 311 complaint types are trending most in the last 5 minutes — 
and what is their historical complaint volume across the full dataset?

## Speed Layer answers:
"What are the top 5 complaint types in the last 5 minutes?"

## Batch Layer answers:
"What are the top 5 complaint types across all 7.35 million records?"

## Serving Layer combines:
"Is what's surging now historically expected or genuinely anomalous?"

## Dataset
- Source: NYC Open Data — 311 Service Requests 2020 to Present
- Rows: 7,350,000 | Columns: 19 | Size: 3.40 GiB
- License: Public Domain 

## Architecture
Kinesis → Lambda + Firehose → DynamoDB + EMR → Flask on EC2

## Team
Ananya & Satvik - NCI MSc Cloud Computing 2026