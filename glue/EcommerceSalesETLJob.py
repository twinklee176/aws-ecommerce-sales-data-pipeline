import sys
import json
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -----------------------------------------------------
# Job Arguments
# -----------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'input_file'
    ]
)

# -----------------------------------------------------
# Glue Context
# -----------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------------------------------
# Paths
# -----------------------------------------------------
INPUT_PATH = args['input_file']

PROCESSED_BUCKET = "s3://twinkle-employee-data-pipeline/processed/"

REPORT_PATH = "s3://twinkle-employee-data-pipeline/reports/data_quality_report/"

# -----------------------------------------------------
# Read CSV
# -----------------------------------------------------
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

rows_read = df.count()

print(f"Rows Read : {rows_read}")

# -----------------------------------------------------
# Remove Duplicate Records
# -----------------------------------------------------
duplicate_rows = rows_read - df.dropDuplicates().count()

df = df.dropDuplicates()

# -----------------------------------------------------
# Remove Null Values
# -----------------------------------------------------
rows_before_null = df.count()

df = df.dropna()

null_rows_removed = rows_before_null - df.count()

# -----------------------------------------------------
# Remove Invalid Quantity
# -----------------------------------------------------
rows_before_quantity = df.count()

df = df.filter(F.col("quantity") > 0)

invalid_quantity = rows_before_quantity - df.count()

# -----------------------------------------------------
# Remove Invalid Revenue
# -----------------------------------------------------
rows_before_revenue = df.count()

df = df.filter(F.col("revenue") >= 0)

invalid_revenue = rows_before_revenue - df.count()

# -----------------------------------------------------
# Remove Invalid Unit Price
# -----------------------------------------------------
rows_before_price = df.count()

df = df.filter(F.col("unit_price") > 0)

invalid_price = rows_before_price - df.count()

# -----------------------------------------------------
# Remove Invalid Rating
# -----------------------------------------------------
rows_before_rating = df.count()

df = df.filter(
    (F.col("customer_rating") >= 0) &
    (F.col("customer_rating") <= 5)
)

invalid_rating = rows_before_rating - df.count()

# -----------------------------------------------------
# Final Row Count
# -----------------------------------------------------
rows_written = df.count()

# -----------------------------------------------------
# Data Quality Report
# -----------------------------------------------------
report = {

    "Rows Read": rows_read,

    "Duplicate Rows Removed": duplicate_rows,

    "Null Rows Removed": null_rows_removed,

    "Invalid Quantity Removed": invalid_quantity,

    "Invalid Revenue Removed": invalid_revenue,

    "Invalid Unit Price Removed": invalid_price,

    "Invalid Rating Removed": invalid_rating,

    "Rows Written": rows_written

}

print(json.dumps(report, indent=4))

# -----------------------------------------------------
# Save Data Quality Report
# -----------------------------------------------------
report_df = spark.createDataFrame(
    [(json.dumps(report, indent=4),)],
    ["Report"]
)

(
    report_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(REPORT_PATH)
)

print("Data Quality Report Saved")

# -----------------------------------------------------
# Write Parquet (Snappy Compression)
# -----------------------------------------------------
(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(PROCESSED_BUCKET)
)

print("Processed Parquet Written Successfully")

# -----------------------------------------------------
# Commit Job
# -----------------------------------------------------
job.commit()

print("Glue ETL Job Completed Successfully")
