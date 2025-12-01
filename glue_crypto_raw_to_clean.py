import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# --------- CONFIG ----------
BUCKET_NAME = "crypto-etl-meghana"   # bucket name
RAW_PREFIX = "raw/"
CLEAN_PREFIX = "clean/crypto_prices/"
RAW_PATH = f"s3://{BUCKET_NAME}/{RAW_PREFIX}"
CLEAN_PATH = f"s3://{BUCKET_NAME}/{CLEAN_PREFIX}"
# ----------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Reading raw JSON from: {RAW_PATH}")

# Read raw JSON files
df_raw = spark.read.json(RAW_PATH)

print("Raw schema:")
df_raw.printSchema()

# JSON structure from generator:
# {
#   "time": {"updatedISO": "..."},
#   "bpi": {
#       "USD": {"code": "USD", "rate_float": ...},
#       "GBP": {"code": "GBP", "rate_float": ...},
#       "EUR": {"code": "EUR", "rate_float": ...}
#   },
#   "fetched_at_utc": "..."
# }

# Select and flatten fields
df_clean = df_raw.select(
    col("fetched_at_utc").alias("fetched_at_utc"),
    col("time.updatedISO").alias("api_time_utc"),
    col("bpi.USD.code").alias("currency_usd"),
    col("bpi.USD.rate_float").alias("price_usd"),
    col("bpi.GBP.code").alias("currency_gbp"),
    col("bpi.GBP.rate_float").alias("price_gbp"),
    col("bpi.EUR.code").alias("currency_eur"),
    col("bpi.EUR.rate_float").alias("price_eur")
)

print("Clean schema:")
df_clean.printSchema()

print(f"Writing cleaned Parquet to: {CLEAN_PATH}")

# Write cleaned data to S3 as Parquet
df_clean.write.mode("overwrite").parquet(CLEAN_PATH)

print("Done writing clean data.")

job.commit()
