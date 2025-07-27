import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, to_timestamp, trim, upper,
    to_date
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# --- Spark Session Setup ---
builder = SparkSession.builder \
    .appName("RawToStdDeltaTransformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- Configs ---
bucket = "tfm-bucket-openair"
raw_prefix = "raw_layer"
std_prefix = "std_layer"

# Table mapping and primary keys
tables = {
    "users": ["id"],
    "billable_hours_summary": ["userId", "start_date", "end_date"]
}

# --- Deduplication logic ---
def deduplicate_latest(df, pk_cols):
    window_spec = Window.partitionBy(*pk_cols).orderBy(col("etl_time").desc())
    return df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# --- Cleaning functions ---
def clean_users(df):
    return (
        df.filter(col("id").isNotNull())
          .withColumn("email", trim(col("email")))
          .withColumn("firstName", upper(trim(col("firstName"))))
          .withColumn("lastName", upper(trim(col("lastName"))))
          .withColumn("etl_time", to_timestamp("etl_time"))
    ).transform(lambda df: deduplicate_latest(df, ["id"]))

def clean_billable_hours(df):
    return (
        df.filter(col("userId").isNotNull())
          .withColumn("total_billable_hours", col("total_billable_hours").cast("double"))
          .filter(col("total_billable_hours") >= 0)
          .withColumn("start_date", to_date("start_date"))
          .withColumn("end_date", to_date("end_date"))
          .withColumn("etl_time", to_timestamp("etl_time"))
    ).transform(lambda df: deduplicate_latest(df, ["userId", "start_date", "end_date"]))


cleaning_functions = {
    "users": clean_users,
    "billable_hours_summary": clean_billable_hours
}

# --- Main Loop ---
for table_name, pk_cols in tables.items():
    print(f"\n🚀 Processing {table_name}")

    try:
        raw_path = f"s3://{bucket}/{raw_prefix}/{table_name}/"
        std_path = f"s3://{bucket}/{std_prefix}/{table_name}/"

        if not DeltaTable.isDeltaTable(spark, raw_path):
            raise Exception(f"Raw Delta table not found at: {raw_path}")

        df_raw = spark.read.format("delta").load(raw_path)
        clean_fn = cleaning_functions.get(table_name)
        df_std = clean_fn(df_raw)

        if DeltaTable.isDeltaTable(spark, std_path):
            print("🔄 Merging into existing STD Delta table...")
            delta_std = DeltaTable.forPath(spark, std_path)
            merge_condition = " AND ".join([f"std.{col} = updates.{col}" for col in pk_cols])

            delta_std.alias("std").merge(
                df_std.alias("updates"),
                merge_condition
            ).whenMatchedUpdate(
                condition="updates.etl_time > std.etl_time",
                set={col: f"updates.{col}" for col in df_std.columns}
            ).whenNotMatchedInsertAll().execute()
        else:
            print("🆕 Writing new STD Delta table...")
            df_std.write.format("delta").mode("overwrite").save(std_path)

        print(f"✅ Done processing {table_name}")

    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")

print("\n🎉 All tables processed.")
