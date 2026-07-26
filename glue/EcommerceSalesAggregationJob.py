import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -------------------------------------------------------
# Job Arguments
# -------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME'
    ]
)

# -------------------------------------------------------
# Glue Context
# -------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------------------------------------
# S3 Paths
# -------------------------------------------------------
# -------------------------------------------------------
# S3 Paths
# -------------------------------------------------------
CURATED_PATH = "s3://twinkle-employee-data-pipeline/curated/"

AGGREGATED_PATH = "s3://twinkle-employee-data-pipeline/aggregated/"

# -------------------------------------------------------
# Read Curated Data
# -------------------------------------------------------
df = spark.read.parquet(CURATED_PATH)

print("Curated dataset loaded successfully.")

# -------------------------------------------------------
# Aggregate Data
# -------------------------------------------------------
aggregated_df = (
    df.groupBy(
        "region",
        "product_category",
        "year",
        "month"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.sum("quantity").alias("total_quantity_sold"),

        F.round(
            F.sum("revenue"),
            2
        ).alias("total_revenue"),

        F.round(
            F.avg("customer_rating"),
            2
        ).alias("average_customer_rating"),

        F.round(
            F.avg("delivery_days"),
            2
        ).alias("average_delivery_days"),

        F.round(
            F.sum("gross_amount"),
            2
        ).alias("gross_sales"),

        F.round(
            F.sum("discount_amount"),
            2
        ).alias("total_discount"),

        F.round(
            F.sum("net_amount"),
            2
        ).alias("net_sales"),

        F.round(
            F.sum("gst"),
            2
        ).alias("total_gst"),

        F.round(
            F.sum("final_amount"),
            2
        ).alias("final_sales"),

        F.round(
            F.avg("profit_margin"),
            2
        ).alias("average_profit_margin")
    )
)

print("Aggregation completed successfully.")

# -------------------------------------------------------
# Sort Results
# -------------------------------------------------------
aggregated_df = aggregated_df.orderBy(
    "year",
    "month",
    "region",
    "product_category"
)

# -------------------------------------------------------
# Write Aggregated Data
# -------------------------------------------------------
(
    aggregated_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(AGGREGATED_PATH)
)

print("Aggregated data written successfully.")

# -------------------------------------------------------
# Display Sample Output
# -------------------------------------------------------
aggregated_df.show(20, truncate=False)

print(f"Total Aggregated Records : {aggregated_df.count()}")

# -------------------------------------------------------
# Commit Glue Job
# -------------------------------------------------------
job.commit()

print("Glue Aggregation Job Completed Successfully.")
