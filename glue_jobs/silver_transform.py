import sys
import uuid

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    to_timestamp, to_date,
    trim, lower, when
)

# ─────────────────────────────────────────
# 1. INIT
# ─────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "source"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

run_date = args["run_date"]
source   = args["source"]
batch_id = str(uuid.uuid4())

print(f"Starting Silver transform")
print(f"run_date : {run_date}")
print(f"batch_id : {batch_id}")

# ─────────────────────────────────────────
# 2. CREATE SILVER DATABASE
# ─────────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.silver_db")
print("silver_db ready")

# ─────────────────────────────────────────
# 3. HELPER FUNCTIONS
# ─────────────────────────────────────────
def add_silver_metadata(df, batch_id, run_date):
    return (df
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_run_date", lit(run_date)))

def write_silver(df, table_name):
    (df.writeTo(f"glue_catalog.silver_db.{table_name}")
        .using("iceberg")
        .tableProperty(
            "location",
            f"s3://retail-lakehouse-silver/iceberg/{table_name}"
        )
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace())
    print(f"  Written: silver_db.{table_name} → {df.count()} rows")

def write_quarantine(df, table_name):
    if df.count() == 0:
        print(f"  Quarantine: {table_name} → 0 bad rows")
        return
    (df.writeTo(f"glue_catalog.silver_db.quarantine_{table_name}")
        .using("iceberg")
        .tableProperty(
            "location",
            f"s3://retail-lakehouse-silver/iceberg/quarantine_{table_name}"
        )
        .tableProperty("format-version", "2")
        .createOrReplace())
    print(f"  Quarantine: {table_name} → {df.count()} bad rows")

# ─────────────────────────────────────────
# 4. TRACK RESULTS
# ─────────────────────────────────────────
success_tables = []
failed_tables  = []

# ─────────────────────────────────────────
# 5. ORDERS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: orders")
try:
    orders = spark.table("glue_catalog.bronze_db.orders")

    VALID_STATUSES = [
        "delivered", "shipped", "canceled",
        "unavailable", "invoiced", "processing",
        "created", "approved"
    ]

    orders = (orders
        .withColumn("order_purchase_timestamp",
            to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",
            to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date",
            to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date",
            to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date",
            to_timestamp("order_estimated_delivery_date"))
        .withColumn("order_status",
            lower(trim(col("order_status"))))
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["order_id"]))

    good = orders.filter(col("order_status").isin(VALID_STATUSES))
    bad  = (orders
        .filter(~col("order_status").isin(VALID_STATUSES))
        .withColumn("quarantine_reason", lit("invalid_order_status"))
        .withColumn("quarantined_at", current_timestamp()))

    good = add_silver_metadata(good, batch_id, run_date)
    write_silver(good, "orders")
    write_quarantine(bad, "orders")
    success_tables.append("orders")

except Exception as e:
    print(f"  FAILED: orders → {str(e)}")
    failed_tables.append({"table": "orders", "error": str(e)})

# ─────────────────────────────────────────
# 6. CUSTOMERS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: customers")
try:
    customers = spark.table("glue_catalog.bronze_db.customers")

    customers = (customers
        .withColumn("customer_zip_code_prefix",
            col("customer_zip_code_prefix").cast("integer"))
        .withColumn("customer_city",
            lower(trim(col("customer_city"))))
        .withColumn("customer_state",
            trim(col("customer_state")))
        .filter(col("customer_id").isNotNull())
        .filter(col("customer_unique_id").isNotNull())
        .dropDuplicates(["customer_id"]))

    customers = add_silver_metadata(customers, batch_id, run_date)
    write_silver(customers, "customers")
    success_tables.append("customers")

except Exception as e:
    print(f"  FAILED: customers → {str(e)}")
    failed_tables.append({"table": "customers", "error": str(e)})

# ─────────────────────────────────────────
# 7. PRODUCTS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: products")
try:
    products = spark.table("glue_catalog.bronze_db.products")

    products = (products
        .withColumn("product_name_lenght",
            col("product_name_lenght").cast("integer"))
        .withColumn("product_description_lenght",
            col("product_description_lenght").cast("integer"))
        .withColumn("product_photos_qty",
            col("product_photos_qty").cast("integer"))
        .withColumn("product_weight_g",
            col("product_weight_g").cast("double"))
        .withColumn("product_length_cm",
            col("product_length_cm").cast("double"))
        .withColumn("product_height_cm",
            col("product_height_cm").cast("double"))
        .withColumn("product_width_cm",
            col("product_width_cm").cast("double"))
        .withColumn("product_category_name",
            lower(trim(col("product_category_name"))))
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["product_id"]))

    products = add_silver_metadata(products, batch_id, run_date)
    write_silver(products, "products")
    success_tables.append("products")

except Exception as e:
    print(f"  FAILED: products → {str(e)}")
    failed_tables.append({"table": "products", "error": str(e)})

# ─────────────────────────────────────────
# 8. ORDER ITEMS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: order_items")
try:
    order_items = spark.table("glue_catalog.bronze_db.order_items")

    order_items = (order_items
        .withColumn("order_item_id",
            col("order_item_id").cast("integer"))
        .withColumn("price",
            col("price").cast("decimal(10,2)"))
        .withColumn("freight_value",
            col("freight_value").cast("decimal(10,2)"))
        .withColumn("shipping_limit_date",
            to_timestamp("shipping_limit_date"))
        .filter(col("order_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("price").cast("decimal(10,2)") > 0)
        .dropDuplicates(["order_id", "order_item_id"]))

    good = order_items.filter(col("price") > 0)
    bad  = (order_items
        .filter(col("price") <= 0)
        .withColumn("quarantine_reason", lit("invalid_price"))
        .withColumn("quarantined_at", current_timestamp()))

    good = add_silver_metadata(good, batch_id, run_date)
    write_silver(good, "order_items")
    write_quarantine(bad, "order_items")
    success_tables.append("order_items")

except Exception as e:
    print(f"  FAILED: order_items → {str(e)}")
    failed_tables.append({"table": "order_items", "error": str(e)})

# ─────────────────────────────────────────
# 9. ORDER PAYMENTS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: order_payments")
try:
    order_payments = spark.table("glue_catalog.bronze_db.order_payments")

    VALID_PAYMENT_TYPES = [
        "credit_card", "boleto",
        "voucher", "debit_card", "not_defined"
    ]

    order_payments = (order_payments
        .withColumn("payment_sequential",
            col("payment_sequential").cast("integer"))
        .withColumn("payment_installments",
            col("payment_installments").cast("integer"))
        .withColumn("payment_value",
            col("payment_value").cast("decimal(10,2)"))
        .withColumn("payment_type",
            lower(trim(col("payment_type"))))
        .filter(col("order_id").isNotNull())
        .filter(col("payment_value").isNotNull())
        .dropDuplicates(["order_id", "payment_sequential"]))

    good = order_payments.filter(
        col("payment_type").isin(VALID_PAYMENT_TYPES)
    )
    bad  = (order_payments
        .filter(~col("payment_type").isin(VALID_PAYMENT_TYPES))
        .withColumn("quarantine_reason", lit("invalid_payment_type"))
        .withColumn("quarantined_at", current_timestamp()))

    good = add_silver_metadata(good, batch_id, run_date)
    write_silver(good, "order_payments")
    write_quarantine(bad, "order_payments")
    success_tables.append("order_payments")

except Exception as e:
    print(f"  FAILED: order_payments → {str(e)}")
    failed_tables.append({"table": "order_payments", "error": str(e)})

# ─────────────────────────────────────────
# 10. ORDER REVIEWS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: order_reviews")
try:
    order_reviews = spark.table("glue_catalog.bronze_db.order_reviews")

    order_reviews = (order_reviews
        .withColumn("review_score",
            col("review_score").cast("integer"))
        .withColumn("review_creation_date",
            to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp",
            to_timestamp("review_answer_timestamp"))
        .filter(col("review_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["review_id"]))

    good = order_reviews.filter(
        col("review_score").between(1, 5)
    )
    bad  = (order_reviews
        .filter(
            col("review_score").isNull() |
            ~col("review_score").between(1, 5)
        )
        .withColumn("quarantine_reason", lit("invalid_review_score"))
        .withColumn("quarantined_at", current_timestamp()))

    good = add_silver_metadata(good, batch_id, run_date)
    write_silver(good, "order_reviews")
    write_quarantine(bad, "order_reviews")
    success_tables.append("order_reviews")

except Exception as e:
    print(f"  FAILED: order_reviews → {str(e)}")
    failed_tables.append({"table": "order_reviews", "error": str(e)})

# ─────────────────────────────────────────
# 11. SELLERS
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: sellers")
try:
    sellers = spark.table("glue_catalog.bronze_db.sellers")

    sellers = (sellers
        .withColumn("seller_zip_code_prefix",
            col("seller_zip_code_prefix").cast("integer"))
        .withColumn("seller_city",
            lower(trim(col("seller_city"))))
        .withColumn("seller_state",
            trim(col("seller_state")))
        .filter(col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"]))

    sellers = add_silver_metadata(sellers, batch_id, run_date)
    write_silver(sellers, "sellers")
    success_tables.append("sellers")

except Exception as e:
    print(f"  FAILED: sellers → {str(e)}")
    failed_tables.append({"table": "sellers", "error": str(e)})

# ─────────────────────────────────────────
# 12. GEOLOCATION
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: geolocation")
try:
    geolocation = spark.table("glue_catalog.bronze_db.geolocation")

    geolocation = (geolocation
        .withColumn("geolocation_zip_code_prefix",
            col("geolocation_zip_code_prefix").cast("integer"))
        .withColumn("geolocation_lat",
            col("geolocation_lat").cast("double"))
        .withColumn("geolocation_lng",
            col("geolocation_lng").cast("double"))
        .withColumn("geolocation_city",
            lower(trim(col("geolocation_city"))))
        .withColumn("geolocation_state",
            trim(col("geolocation_state")))
        .filter(col("geolocation_zip_code_prefix").isNotNull())
        .filter(col("geolocation_lat").between(-90, 90))
        .filter(col("geolocation_lng").between(-180, 180))
        .dropDuplicates([
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ]))

    geolocation = add_silver_metadata(geolocation, batch_id, run_date)
    write_silver(geolocation, "geolocation")
    success_tables.append("geolocation")

except Exception as e:
    print(f"  FAILED: geolocation → {str(e)}")
    failed_tables.append({"table": "geolocation", "error": str(e)})

# ─────────────────────────────────────────
# 13. PRODUCT CATEGORY TRANSLATION
# ─────────────────────────────────────────
print("\n" + "="*50)
print("Processing: product_category_name_translation")
try:
    translation = spark.table(
        "glue_catalog.bronze_db.product_category_name_translation"
    )

    translation = (translation
        .withColumn("product_category_name",
            lower(trim(col("product_category_name"))))
        .withColumn("product_category_name_english",
            lower(trim(col("product_category_name_english"))))
        .filter(col("product_category_name").isNotNull())
        .dropDuplicates(["product_category_name"]))

    translation = add_silver_metadata(translation, batch_id, run_date)
    write_silver(translation, "product_category_name_translation")
    success_tables.append("product_category_name_translation")

except Exception as e:
    print(f"  FAILED: product_category_name_translation → {str(e)}")
    failed_tables.append({
        "table": "product_category_name_translation",
        "error": str(e)
    })

# ─────────────────────────────────────────
# 14. SUMMARY
# ─────────────────────────────────────────
print(f"\n{'='*50}")
print(f"SILVER TRANSFORM COMPLETE")
print(f"Succeeded : {len(success_tables)} → {success_tables}")
print(f"Failed    : {len(failed_tables)}")
for f in failed_tables:
    print(f"  → {f['table']}: {f['error']}")

if failed_tables:
    raise Exception(
        f"Silver transform failed for: "
        f"{[f['table'] for f in failed_tables]}"
    )

job.commit()