# AWS Serverless Data Pipeline - Setup Guide

This guide explains how to deploy and run the **AWS Serverless Data Pipeline for E-commerce Sales Analytics** on AWS. Follow the steps below to recreate the complete project.

---

# Prerequisites

Before starting, ensure you have:

- An AWS Account
- IAM user with appropriate permissions
- Python 3.x
- AWS CLI (optional)
- E-commerce sales dataset (CSV format)

AWS services used:

- Amazon S3
- AWS Lambda
- AWS Glue
- AWS Glue Crawler
- AWS Glue Data Catalog
- Amazon EventBridge
- Amazon CloudWatch
- Amazon SNS
- Amazon Athena
- AWS IAM

---

# Part 1 – Create Storage Layers

Create the storage layers using either separate S3 buckets or folders within a single bucket.

Example:

```
landing/
raw/
processed/
curated/
reports/
aggregated/
```

Configure the S3 bucket:

- Enable **Block Public Access**
- Enable **Server-side Encryption (SSE-S3)**
- Enable **Bucket Versioning**

These settings improve security and protect stored data.

---

# Part 2 – Configure Data Ingestion

Create an AWS Lambda function named:

```
EcommercePipelineValidationLambda
```

Upload the Lambda source code from the `lambda/` folder.

Configure an **Amazon S3 Event Notification** on the Landing Zone.

Trigger:

```
Object Created (PUT)
```

Whenever a CSV file is uploaded, the Lambda function should:

- Validate the file extension
- Verify required columns
- Check for empty files
- Copy the validated file to the Raw Zone
- Write execution logs to Amazon CloudWatch
- Start the AWS Glue ETL Job

Upload a sample CSV file to verify that the Lambda function executes successfully.

---

# Part 3 – Create the Glue ETL Job

Create a Glue Job named:

```
EcommerceSalesETLJob
```

Upload the ETL script from the `glue/` folder.

Configure:

- IAM Role
- Glue Version
- Spark Job
- Script Location
- Worker Type
- Number of Workers

The ETL job should:

- Read data from the Raw Zone
- Remove duplicate records
- Remove rows with missing mandatory fields
- Remove invalid records
- Generate a Data Quality Report
- Convert CSV files to Parquet
- Apply Snappy Compression
- Store the output in the Processed Zone

Run the job and verify that the processed Parquet files are created successfully.

---

# Part 4 – Create the Glue Transformation Job

Create another Glue Job named:

```
EcommerceSalesTransformationJob
```

Upload the transformation script.

This job should:

- Read data from the Processed Zone
- Perform business transformations
- Calculate Gross Amount
- Calculate Discount Amount
- Calculate Net Amount
- Calculate GST
- Calculate Final Amount
- Calculate Profit Margin
- Create Year and Month columns
- Write the transformed data to the Curated Zone

Verify that the transformed dataset is written successfully.

---

# Part 5 – Create Metadata

Create a Glue Database.

Example:

```
ecommerce_database
```

Create a Glue Crawler.

Crawler configuration:

- Data Source → Curated Zone
- IAM Role → Glue Crawler Role
- Database → ecommerce_database
- Schedule → On Demand (or Scheduled)

Run the crawler.

After completion, verify that the generated table appears in the AWS Glue Console under **Tables**.

---

# Part 6 – Configure Monitoring

Open Amazon CloudWatch.

Verify that logs are generated for:

- Validation Lambda
- Glue ETL Job
- Glue Transformation Job
- Glue Aggregation Job

Create a CloudWatch Alarm.

Example:

```
Alarm Name:
GlueJobFailureAlarm
```

Monitor:

- Failed Glue Job Executions

Configure the alarm to trigger whenever a Glue Job fails.

Verify that the alarm changes state when a failure occurs.

---

# Part 7 – Configure Notifications

Create an Amazon SNS Topic.

Example:

```
EcommercePipelineNotifications
```

Subscribe your email address to the topic.

Confirm the subscription from the email received.

Configure notifications for:

- Successful Glue Job execution
- Failed Glue Job execution
- Lambda execution failure

Run the pipeline and verify that notification emails are received.

---

# Part 8 – Configure Scheduled Execution

Open Amazon EventBridge.

Create scheduled rules for automation.

### Rule 1

Schedule the Glue Crawler.

Example:

```
Runs once every day
```

Target:

```
Glue Crawler
```

---

### Rule 2

Schedule the Glue ETL Job.

Example:

```
Runs every day at a specified time
```

Target:

```
EcommerceSalesETLJob
```

Save both rules.

Verify that the schedule is enabled and the targets are correctly configured.

---

# Running the Project

1. Upload the sample CSV dataset to the Landing Zone.
2. Verify that the Validation Lambda is triggered.
3. Confirm the file is copied to the Raw Zone.
4. Monitor the ETL Job execution.
5. Verify that Parquet files are created in the Processed Zone.
6. Verify that the Transformation Job completes successfully.
7. Confirm that transformed data is written to the Curated Zone.
8. Run the Glue Crawler and verify that tables are created.
9. Execute SQL queries in Amazon Athena.
10. Verify CloudWatch logs and SNS notifications.

---

# Expected Outputs

After successful execution, the following outputs should be available:

- Validated CSV files
- Processed Parquet dataset
- Curated dataset
- Aggregated analytics dataset
- Data Quality Report
- Glue Data Catalog tables
- Athena query results
- CloudWatch logs
- SNS notification emails

---
# Verification Checklist

- S3 Storage Layers Created
- S3 Event Notification Configured
- Validation Lambda Executed Successfully
- File Copied to Raw Zone
- Glue ETL Job Completed
- Processed Parquet Files Generated
- Glue Transformation Job Completed
- Curated Dataset Created
- Glue Database Created
- Glue Crawler Completed
- Glue Data Catalog Updated
- CloudWatch Logs Generated
- CloudWatch Alarm Configured
- SNS Notifications Received
- EventBridge Scheduled Rules Created
- Athena Queries Executed Successfully
