# AWS Serverless Data Pipeline for E-commerce Sales Analytics

# Project Overview

The **AWS Serverless Data Pipeline for E-commerce Sales Analytics** is an end-to-end data engineering project that automates the processing of e-commerce sales data using AWS serverless services.

## Key Features

- Built a **fully automated serverless data pipeline** using AWS managed services.
- Designed an **event-driven architecture** that automatically processes files uploaded to Amazon S3.
- Validated uploaded CSV files using **AWS Lambda** by checking:
  - File format
  - Required columns
  - Empty files
- Copied validated files from the **Landing Bucket** to the **Raw Bucket**.
- Automated ETL processing using **AWS Glue** to:
  - Remove duplicate records
  - Remove null values
  - Filter invalid records
  - Generate data quality reports
  - Convert CSV files to Parquet format
  - Apply Snappy compression
- Performed business data transformations, including:
  - Gross Amount calculation
  - Discount Amount calculation
  - Net Amount calculation
  - GST calculation
  - Final Amount calculation
  - Profit Margin calculation
  - Year and Month partitioning
- Stored transformed datasets in an optimized **Curated S3 Bucket**.
- Used **Amazon EventBridge** to orchestrate each stage of the pipeline automatically.
- Updated the **AWS Glue Data Catalog** using a Glue Crawler for metadata management.
- Generated aggregated business insights such as:
  - Revenue by Region
  - Revenue by Product Category
  - Monthly Revenue
  - Total Orders
  - Average Customer Rating
  - Delivery Performance
- Queried processed datasets using **Amazon Athena** for SQL-based analytics.
- Monitored pipeline execution with **Amazon CloudWatch Logs**.
- Sent pipeline execution notifications using **Amazon SNS**.
- Demonstrated cloud-native data engineering practices, including:
  - Serverless Computing
  - Event-Driven Processing
  - ETL Automation
  - Data Quality Validation
  - Metadata Cataloging
  - Analytical Reporting
- Built a scalable, cost-effective, and production-ready solution using AWS serverless technologies.

---

# Project Highlights

- Fully Serverless Architecture
- Event-Driven Workflow
- Automatic CSV Validation
- Automated ETL Processing
- Business Data Transformation
- Automatic Glue Crawler Execution
- Automatic Aggregation Pipeline
- Amazon Athena Integration
- CloudWatch Monitoring
- SNS Notifications
- Data Quality Reporting
- CSV to Parquet Conversion
- Snappy Compression
- Year & Month Partitioning

---

# Architecture

# Architecture

```text
Upload CSV File
      │
      ▼
S3 Landing Bucket
      │
      ▼
Validation Lambda
      │
      ├── Validate CSV
      ├── Check Required Columns
      ├── Copy to Raw Bucket
      └── Start Glue ETL Job
      │
      ▼
S3 Raw Bucket
      │
      ▼
Glue ETL Job
      │
      ├── Remove Duplicates
      ├── Remove Null Values
      ├── Remove Invalid Records
      ├── Generate Data Quality Report
      └── Convert CSV → Parquet
      │
      ▼
S3 Processed Bucket
      │
      ▼
EventBridge Rule
      │
      ▼
Start Transformation Lambda
      │
      ▼
Glue Transformation Job
      │
      ├── Gross Amount
      ├── Discount Amount
      ├── Net Amount
      ├── GST Calculation
      ├── Final Amount
      ├── Profit Margin
      └── Year & Month Partitioning
      │
      ▼
S3 Curated Bucket
      │
      ▼
EventBridge Rule
      │
      ▼
Start Glue Crawler Lambda
      │
      ▼
Glue Crawler
      │
      ▼
Glue Data Catalog
      │
      ▼
EventBridge Rule
      │
      ▼
Start Aggregation Lambda
      │
      ▼
Glue Aggregation Job
      │
      ├── Revenue by Region
      ├── Revenue by Category
      ├── Monthly Revenue
      ├── Average Rating
      ├── Delivery Analytics
      └── Total Orders
      │
      ▼
S3 Aggregated Bucket
      │
      ▼
Amazon Athena
```

---

# AWS Services Used

- Amazon S3
- AWS Lambda
- AWS Glue ETL
- AWS Glue Crawler
- AWS Glue Data Catalog
- Amazon EventBridge
- Amazon Athena
- Amazon SNS
- Amazon CloudWatch
- AWS IAM

---

# Repository Structure

```
aws-serverless-data-pipeline/
│
├── README.md
|
├── dataset/
│   └── ecommerce_sales_analytics.csv
│
├── lambda/
│   ├── EcommercePipelineValidationLambda.py
│   ├── StartTransformationLambda.py
│   ├── StartCrawlerLambda.py
│   └── StartAggregationLambda.py
│
├── glue/
│   ├── EcommerceSalesETLJob.py
│   ├── EcommerceSalesTransformationJob.py
│   └── EcommerceSalesAggregationJob.py

```

---

# Lambda Functions

## 1. EcommercePipelineValidationLambda

Responsibilities

- Triggered automatically by Amazon S3
- Validates uploaded CSV files
- Validates required schema
- Checks empty files
- Copies validated files to Raw Bucket
- Starts Glue ETL Job
- Sends SNS notifications
- Writes CloudWatch logs

---

## 2. StartTransformationLambda

Responsibilities

- Triggered by EventBridge
- Starts EcommerceSalesTransformationJob
- Logs Glue Job Run ID

---

## 3. StartCrawlerLambda

Responsibilities

- Triggered by EventBridge
- Starts EcommerceSalesCrawler
- Updates Glue Data Catalog

---

## 4. StartAggregationLambda

Responsibilities

- Triggered by EventBridge
- Starts EcommerceSalesAggregationJob
- Generates analytics dataset

---

# Glue Jobs

## EcommerceSalesETLJob

Performs

- Read CSV
- Remove duplicates
- Remove null values
- Remove invalid quantity
- Remove invalid revenue
- Remove invalid unit price
- Remove invalid customer ratings
- Generate Data Quality Report
- Convert CSV to Parquet
- Snappy Compression
- Store processed dataset

---

## EcommerceSalesTransformationJob

Performs

- Convert Order Date
- Calculate Gross Amount
- Calculate Discount Amount
- Calculate Net Amount
- Calculate GST
- Calculate Final Amount
- Calculate Profit Margin
- Delivery Category
- Create Year column
- Create Month column
- Partition Curated Dataset

---

## EcommerceSalesAggregationJob

Performs

- Revenue by Region
- Revenue by Product Category
- Monthly Revenue
- Average Customer Rating
- Average Delivery Days
- Total Revenue
- Total Orders
- Total Quantity Sold

---

# EventBridge Automation

## Rule 1

```
Glue ETL Job
      ↓
Succeeded
      ↓
StartTransformationLambda
```

---

## Rule 2

```
Glue Transformation Job
           ↓
Succeeded
           ↓
StartCrawlerLambda
```

---

## Rule 3

```
Glue Crawler
      ↓
Succeeded
      ↓
StartAggregationLambda
```

---

# Project Workflow

1. Upload CSV file to Landing Bucket.
2. Amazon S3 triggers Validation Lambda.
3. Lambda validates file and copies it to Raw Bucket.
4. Lambda starts Glue ETL Job.
5. ETL Job cleans data and converts it to Parquet.
6. EventBridge detects successful ETL completion.
7. Transformation Lambda starts Glue Transformation Job.
8. Transformation Job enriches business data and stores partitioned output.
9. EventBridge triggers Glue Crawler.
10. Glue Crawler updates the Glue Data Catalog.
11. EventBridge triggers Aggregation Lambda.
12. Aggregation Job creates analytical datasets.
13. Amazon Athena queries curated and aggregated data.

---

# Additional Libraries Used

## Python Libraries

- boto3
- json
- csv
- io
- logging

## PySpark Libraries

- pyspark.sql.functions
- SparkContext

## AWS Glue Libraries

- GlueContext
- Job
- getResolvedOptions

---

# Data Processing Features

- CSV Validation
- Schema Validation
- Duplicate Removal
- Null Value Removal
- Invalid Record Removal
- Data Quality Report
- CSV to Parquet Conversion
- Snappy Compression
- Business Calculations
- Partitioned Storage
- Aggregated Analytics
- Athena Ready Dataset

---

# Project Outputs

- Raw Dataset
- Processed Parquet Dataset
- Curated Dataset
- Aggregated Dataset
- Data Quality Report
- Glue Data Catalog Table
- Athena Query Results
- CloudWatch Logs
- SNS Notifications

---

# Future Enhancements

- Amazon QuickSight Dashboard
- Infrastructure as Code using Terraform
- CI/CD using GitHub Actions or AWS CodePipeline
- Incremental ETL using Glue Job Bookmarks
- Data Quality Dashboard

---

# Author

**Twinkle Mahato**

Computer Science Engineering Student

AWS Data Engineering & DevOps Student

---

