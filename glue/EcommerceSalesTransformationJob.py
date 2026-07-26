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
PROCESSED_PATH = "s3://twinkle-employee-data-pipeline/processed/"

CURATED_PATH = "s3://twinkle-employee-data-pipeline/curated/"

# -------------------------------------------------------
# Read Processed Parquet
# -------------------------------------------------------
df = spark.read.parquet(PROCESSED_PATH)

print("Processed data loaded successfully.")

# -------------------------------------------------------
# Convert Order Date to Date Type
# -------------------------------------------------------
df = df.withColumn(
    "order_date",
    F.to_date(F.col("order_date"), "M/d/yyyy")
)
print("Checking converted dates:")
df.select("order_date").show(10, False)
# -------------------------------------------------------
# Gross Amount
# -------------------------------------------------------
df = df.withColumn(
    "gross_amount",
    F.round(
        F.col("quantity") * F.col("unit_price"),
        2
    )
)

# -------------------------------------------------------
# Discount Amount
# -------------------------------------------------------
df = df.withColumn(
    "discount_amount",
    F.round(
        F.col("gross_amount") * F.col("discount"),
        2
    )
)

# -------------------------------------------------------
# Net Amount
# -------------------------------------------------------
df = df.withColumn(
    "net_amount",
    F.round(
        F.col("gross_amount") -
        F.col("discount_amount"),
        2
    )
)

# -------------------------------------------------------
# GST (18%)
# -------------------------------------------------------
df = df.withColumn(
    "gst",
    F.round(
        F.col("net_amount") * 0.18,
        2
    )
)

# -------------------------------------------------------
# Final Amount
# -------------------------------------------------------
df = df.withColumn(
    "final_amount",
    F.round(
        F.col("net_amount") +
        F.col("gst"),
        2
    )
)

# -------------------------------------------------------
# Profit Margin
# -------------------------------------------------------
df = df.withColumn(
    "profit_margin",
    F.when(
        F.col("gross_amount") != 0,
        F.round(
            (F.col("revenue") / F.col("gross_amount")) * 100,
            2
        )
    ).otherwise(0)
)

# -------------------------------------------------------
# Delivery Category
# -------------------------------------------------------
df = df.withColumn(
    "delivery_category",
    F.when(F.col("delivery_days") <= 2, "Fast")
     .when(F.col("delivery_days") <= 5, "Medium")
     .otherwise("Slow")
)

# -------------------------------------------------------
# Year & Month Columns
# -------------------------------------------------------
df = df.withColumn(
    "year",
    F.year("order_date")
)

df = df.withColumn(
    "month",
    F.month("order_date")
)

print("Transformation completed successfully.")

# -------------------------------------------------------
# Write Curated Data
# -------------------------------------------------------
(
    df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .parquet(CURATED_PATH)
)

print("Curated data written successfully.")

# -------------------------------------------------------
# Display Sample Data
# -------------------------------------------------------
df.show(10, truncate=False)

print(f"Total Records Written : {df.count()}")

# -------------------------------------------------------
# Commit Job
# -------------------------------------------------------
job.commit()

print("Glue Transformation Job Completed Successfully.")
