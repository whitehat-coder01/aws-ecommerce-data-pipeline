# Scalable Event-Driven eCommerce Data Pipeline Using AWS

## Project Overview

This project implements an event-driven eCommerce data pipeline using Python and AWS services.

The pipeline ingests eCommerce order events, processes them in near real time, validates incoming records, separates valid and invalid data, prevents duplicate processing, and makes clean data available for analytical queries.

The project demonstrates practical implementation of cloud-based data engineering concepts, including streaming ingestion, serverless processing, data quality, fault handling, idempotency, data storage, and SQL analytics.

## Business Problem

An eCommerce platform generates a large number of order events.

A data engineering system needs to:

- Ingest incoming order events.
- Validate order information.
- Identify invalid records.
- Prevent duplicate processing.
- Store processed data reliably.
- Support analytical queries on business data.
- Monitor failures and pipeline performance.

This project addresses these requirements using AWS services.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data generation and processing |
| Boto3 | AWS service integration |
| Amazon Kinesis Data Streams | Streaming data ingestion |
| AWS Lambda | Event processing |
| Amazon S3 | Processed and quarantined data storage |
| Amazon DynamoDB | Order ledger and idempotency |
| Amazon Athena | SQL-based analytics |
| Amazon SQS | Failed-event destination |
| Amazon CloudWatch | Monitoring and logging |
| Amazon SNS | Notifications |
| Pytest | Automated testing |
| Moto | Mock AWS services for testing |

## Project Status

Core pipeline implementation and automated tests completed.

- Order validation implemented.
- S3 clean and quarantine storage implemented.
- DynamoDB lease-based idempotency implemented.
- Kinesis partial batch failure handling implemented.
- CloudWatch monitoring and SNS notifications configured.
- Automated tests: 21 passing.

---

## System Architecture

The pipeline follows an event-driven architecture.

### Architecture Diagram

```text
              Python Producer
                    |
                    v
          Amazon Kinesis Data Streams
                    |
                    v
               AWS Lambda
                    |
          +---------+---------+
          |                   |
          v                   v
    Data Validation      DynamoDB Ledger
          |
     +----+----+
     |         |
   Valid     Invalid
     |         |
     v         v
  Amazon S3  Amazon S3
  Clean Data Quarantine
     |
     v
  Amazon Athena
     |
     v
  SQL Analytics
```

### Supporting AWS Services

- **Amazon SQS:** Receives failure destination metadata when Kinesis processing retries are exhausted.
- **Amazon CloudWatch:** Collects Lambda logs, metrics, and monitoring information.
- **Amazon SNS:** Sends notifications based on CloudWatch alarms.

## Data Flow

### 1. Data Generation

A Python producer generates synthetic eCommerce order events containing:

- Order ID
- Customer ID
- Product ID
- Quantity
- Price
- Total amount
- Event timestamp

The producer publishes events to Amazon Kinesis Data Streams using Boto3.

### 2. Streaming Ingestion

Amazon Kinesis Data Streams receives incoming order events and makes them available to the Lambda consumer.

### 3. Data Processing

AWS Lambda processes Kinesis records.

The function:

- Decodes Base64-encoded event data.
- Parses JSON records.
- Validates mandatory fields.
- Checks quantity and price.
- Verifies total amount.
- Validates event timestamps.

### 4. Data Quality and Routing

Valid records are stored in:

`processed/athena-ready/`

Invalid records are stored in:

`processed/quarantine/`

This separation prevents invalid records from entering the analytics dataset.

### 5. Idempotency and Order Tracking

Amazon DynamoDB maintains an order-processing ledger.

The implementation uses:

- Conditional writes.
- PROCESSING and COMPLETE statuses.
- Lease expiration.
- Unique lease tokens.

These mechanisms help prevent duplicate processing and allow recovery of abandoned processing claims.

### 6. Analytics

Amazon Athena queries the clean JSON data stored in S3.

Example business metrics include:

- Total orders.
- Daily revenue.
- Customer-wise revenue.
- Product-wise sales.
- Order status distribution.

### 7. Monitoring and Failure Handling

The pipeline uses CloudWatch, SNS, and SQS for monitoring, notifications, and failure handling.

Kinesis partial batch failure reporting allows individual failed records to be retried instead of unnecessarily retrying an entire batch.



---

## SQL Analytics Using Amazon Athena

Amazon Athena is used to analyze processed eCommerce order data stored in Amazon S3.

Database:

`ecommerce_analytics`

Table:

`orders_clean`

### 1. Total Number of Orders

```sql
SELECT COUNT(*) AS total_orders
FROM ecommerce_analytics.orders_clean;
```

### 2. Daily Revenue

```sql
SELECT
    order_date,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS daily_revenue
FROM ecommerce_analytics.orders_clean
GROUP BY order_date
ORDER BY order_date;
```

### 3. Customer-wise Revenue

```sql
SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_revenue
FROM ecommerce_analytics.orders_clean
GROUP BY customer_id
ORDER BY total_revenue DESC;
```

### 4. Product-wise Sales

```sql
SELECT
    product_id,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_revenue
FROM ecommerce_analytics.orders_clean
GROUP BY product_id
ORDER BY total_revenue DESC;
```

### 5. Order Status Distribution

```sql
SELECT
    status,
    COUNT(*) AS total_orders
FROM ecommerce_analytics.orders_clean
GROUP BY status
ORDER BY total_orders DESC;
```

## Data Quality and Reliability

The pipeline includes validation and failure-handling mechanisms.

| Scenario | Handling mechanism |
|---|---|
| Missing mandatory fields | Reject and quarantine |
| Invalid quantity | Reject and quarantine |
| Invalid price | Reject and quarantine |
| Incorrect total amount | Reject and quarantine |
| Invalid timestamp | Reject and quarantine |
| Duplicate order | DynamoDB conditional claim |
| Abandoned processing claim | Lease recovery |
| S3 processing failure | Kinesis partial batch retry |
| Repeated processing | Lease-token ownership checks |

## Automated Testing

Automated tests were developed using Pytest and Moto.

Test coverage includes:

- Order validation.
- Invalid data handling.
- DynamoDB conditional writes.
- Duplicate order prevention.
- Lease expiration and recovery.
- Lease ownership protection.
- Lambda handler execution.
- S3 failure handling.
- Quarantine functionality.

**Latest recorded test result: 21 passed.**

These tests use mocked AWS services. Actual AWS integration and production-scale load testing are separate validation steps.


---

## Project Structure

```text
aws-ecommerce-data-pipeline/
│
├── producer/
│   └── producer.py
│
├── lambda/
│   └── lambda_function.py
│
├── tests/
│   ├── test_validation.py
│   ├── test_idempotency.py
│   └── test_lambda_handler.py
│
├── sql/
│   └── analytics_queries.sql
│
├── README.md
└── requirements.txt
```

*Note: Update this structure to match the actual files in your repository.*

## Installation and Setup

### Prerequisites

- Python 3.12 or later.
- AWS account.
- AWS CLI.
- AWS IAM permissions for the required services.
- Git.

### 1. Clone the Repository

```bash
git clone https://github.com/whitehat-coder01/aws-ecommerce-data-pipeline
cd aws-ecommerce-data-pipeline
```

### 2. Create a Virtual Environment

Windows:

```powershell
python -m venv venv
venv\Scripts\activate
```

### 3. Install Dependencies

```powershell
python -m pip install -r requirements.txt
```

### 4. Configure AWS Credentials

Configure AWS CLI locally:

```powershell
aws configure
```

Use your own AWS credentials securely. Never commit credentials or secret keys to GitHub.

### 5. Run Automated Tests

```powershell
python -m pytest tests/ -v
```

### 6. Run the Data Producer

```powershell
python producer/kinesis_producer.py
```

The producer requires a configured Kinesis stream and appropriate AWS permissions.

### 7. Deploy the Lambda Function

Deploy `lambda/lambda_function.py` to AWS Lambda with the required dependencies, IAM permissions, and Kinesis event source mapping.

Configure the S3 bucket, DynamoDB table, and other environment-specific settings before deployment.

## Future Improvements

- Infrastructure as Code using AWS CloudFormation or Terraform.
- CI/CD integration using GitHub Actions.
- Schema evolution and schema validation.
- End-to-end AWS integration testing.
- Performance and load testing.
- Data partitioning and Parquet conversion for efficient Athena queries.
- Enhanced pipeline cost monitoring.
