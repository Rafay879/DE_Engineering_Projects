import sys
import uuid

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, input_file_name, lit
 # Updated: test deploy workflow
# 1. INIT
args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "source"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 2. JOB VARIABLES
run_date = args["run_date"]
source   = args["source"]
batch_id = str(uuid.uuid4())

print(f"Starting Bronze ingestion")
print(f"run_date : {run_date}")
print(f"batch_id : {batch_id}")
print(f"source   : {source}")

# Iceberg configs are now set via Glue job parameters
# --conf spark.sql.extensions=...
# --datalake-formats iceberg

# 3. TABLE SCHEMA CONTRACTS
TABLES = {
    "orders": [
        "order_id",
        "customer_id",
        "order_status"
    ],
    "customers": [
        "customer_id",
        "customer_city"
    ],
    "products": [
        "product_id",
        "product_category_name"
    ],
    "order_items": [
        "order_id",
        "product_id",
        "price"
    ],
    "order_payments": [
        "order_id",
        "payment_value"
    ],
    "order_reviews": [
        "review_id",
        "order_id"
    ],
    "sellers": [
        "seller_id"
    ],
    "geolocation": [
        "geolocation_zip_code_prefix"
    ],
    "product_category_name_translation": [
        "product_category_name"
    ],
}

# 4. CREATE BRONZE DATABASE
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.bronze_db")
print("bronze_db ready")

# 5. MAIN LOOP
success_tables = []
failed_tables  = []

for table_name, required_cols in TABLES.items():

    print(f"\n{'='*50}")
    print(f"Processing: {table_name}")

    try:
        # 5a. READ CSV
        s3_path = (
            f"s3://retail-lakehouse-bronze/raw/olist/"
            f"run_date={run_date}/"
            f"olist_{table_name}_dataset.csv"
        )

        df = (spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(s3_path))

        print(f"  Read from: {s3_path}")

        # 5b. SCHEMA CHECK
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(
                f"Schema drift in {table_name}: missing {missing}"
            )
        print(f"  Schema contract passed")

        # 5c. EMPTY CHECK
        row_count = df.count()
        if row_count == 0:
            raise ValueError(f"Empty source file: {table_name}")
        print(f"  Row count: {row_count}")

        # 5d. ADD METADATA
        df = (df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_batch_id", lit(batch_id))
            .withColumn("_run_date", lit(run_date)))

        # 5e. WRITE ICEBERG
        (df.writeTo(f"glue_catalog.bronze_db.{table_name}")
            .using("iceberg")
            .tableProperty(
                "location",
                f"s3://retail-lakehouse-bronze/iceberg/{table_name}"
            )
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .createOrReplace())

        print(f"  Iceberg table written successfully")
        success_tables.append(table_name)

    except Exception as e:
        print(f"  FAILED: {table_name} → {str(e)}")
        failed_tables.append({
            "table": table_name,
            "error": str(e)
        })
        continue

# 6. SUMMARY
print(f"\n{'='*50}")
print(f"BRONZE INGESTION COMPLETE")
print(f"Succeeded : {len(success_tables)} → {success_tables}")
print(f"Failed    : {len(failed_tables)}")
for f in failed_tables:
    print(f"  → {f['table']}: {f['error']}")

if failed_tables:
    raise Exception(
        f"Bronze ingestion failed for: "
        f"{[f['table'] for f in failed_tables]}"
    )

job.commit()