import json
import boto3
import logging
import csv
import io

# -----------------------------
# AWS Clients
# -----------------------------
s3 = boto3.client("s3")
glue = boto3.client("glue")
sns = boto3.client("sns")

# -----------------------------
# Configuration
# -----------------------------
RAW_BUCKET = "twinkle-employee-data-pipeline"

GLUE_JOB_NAME = "EcommerceSalesETLJob"

SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:796093524638:EcommercePipelineNotifications"

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------------
# Required Dataset Columns
# -----------------------------
REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_category",
    "region",
    "quantity",
    "unit_price",
    "discount",
    "payment_method",
    "delivery_days",
    "customer_rating",
    "revenue"
]

# -------------------------------------------------------
# SNS Notification
# -------------------------------------------------------
def send_notification(subject, message):
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        logger.error(f"SNS Error: {str(e)}")

# -------------------------------------------------------
# Lambda Handler
# -------------------------------------------------------
def lambda_handler(event, context):

    try:

        logger.info("========== Lambda Triggered ==========")

        record = event["Records"][0]

        landing_bucket = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]

        logger.info(f"Bucket : {landing_bucket}")
        logger.info(f"Incoming File : {file_key}")

        # ---------------------------------------------------
        # Process only landing folder
        # ---------------------------------------------------
        if not file_key.startswith("landing/"):
            logger.info("File is not inside landing folder. Ignoring.")
            return {
                "statusCode": 200,
                "body": "Ignored"
            }

        # ---------------------------------------------------
        # Validate File Extension
        # ---------------------------------------------------
        if not file_key.lower().endswith(".csv"):

            message = f"Invalid File Type: {file_key}"

            logger.error(message)

            send_notification(
                "File Validation Failed",
                message
            )

            return {
                "statusCode": 400,
                "body": message
            }

        # ---------------------------------------------------
        # Read File
        # ---------------------------------------------------
        response = s3.get_object(
            Bucket=landing_bucket,
            Key=file_key
        )

        file_data = response["Body"].read()

        # ---------------------------------------------------
        # Empty File Validation
        # ---------------------------------------------------
        if len(file_data) == 0:

            message = "Uploaded CSV is empty."

            logger.error(message)

            send_notification(
                "Empty CSV Uploaded",
                message
            )

            return {
                "statusCode": 400,
                "body": message
            }

        # ---------------------------------------------------
        # Read CSV Header
        # ---------------------------------------------------
        csv_content = file_data.decode("utf-8")

        csv_reader = csv.reader(io.StringIO(csv_content))

        header = next(csv_reader)

        missing_columns = [
            col for col in REQUIRED_COLUMNS
            if col not in header
        ]

        if missing_columns:

            message = (
                "Schema Validation Failed.\n"
                f"Missing Columns: {missing_columns}"
            )

            logger.error(message)

            send_notification(
                "Schema Validation Failed",
                message
            )

            return {
                "statusCode": 400,
                "body": message
            }

        logger.info("Schema Validation Successful")

        # ---------------------------------------------------
        # Copy Landing -> Raw
        # ---------------------------------------------------
        filename = file_key.split("/")[-1]

        raw_key = f"raw/{filename}"

        copy_source = {
            "Bucket": landing_bucket,
            "Key": file_key
        }

        s3.copy_object(
            Bucket=RAW_BUCKET,
            CopySource=copy_source,
            Key=raw_key
        )

        logger.info(f"Copied to {raw_key}")

        # ---------------------------------------------------
        # Trigger Glue ETL Job
        # ---------------------------------------------------
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--input_file": f"s3://{RAW_BUCKET}/{raw_key}"
            }
        )

        job_run_id = response["JobRunId"]

        logger.info(f"Glue Job Started: {job_run_id}")

        send_notification(
            "Pipeline Started",
            f"""
Pipeline started successfully.

Uploaded File:
{file_key}

Copied To:
{raw_key}

Glue Job:
{GLUE_JOB_NAME}

Job Run ID:
{job_run_id}
"""
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Validation Successful",
                "raw_file": raw_key,
                "GlueJobRunID": job_run_id
            })
        }

    except Exception as e:

        logger.exception(str(e))

        send_notification(
            "Lambda Execution Failed",
            str(e)
        )

        return {
            "statusCode": 500,
            "body": str(e)
        }
