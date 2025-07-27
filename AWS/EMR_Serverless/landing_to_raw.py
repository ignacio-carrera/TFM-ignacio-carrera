import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# --- Spark Session Setup ---
spark = configure_spark_with_delta_pip(
    SparkSession.builder
        .appName("LandingToRawDeltaIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# --- AWS SDK Client ---
s3_client = boto3.client("s3")

# --- Configs (can be replaced with sys.argv or env vars) ---
bucket = "tfm-bucket-openair"
landing_prefix = "landing"
raw_prefix = "raw_layer"
tables = {
    "users": "id",
    "billable_hours_summary": "userId"
}

# --- Helpers ---
def get_all_csv_paths(table_name):
    prefix = f"{landing_prefix}/{table_name}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = response.get("Contents", [])
    return [f"s3://{bucket}/{obj['Key']}" for obj in files if obj["Key"].endswith(".csv")]

# --- Main Processing Loop ---
for table_name in tables:
    print(f"\n🚀 Processing table: {table_name}")
    try:
        csv_paths = get_all_csv_paths(table_name)
        if not csv_paths:
            raise Exception("No CSV files found.")

        print(f"📁 Found {len(csv_paths)} files.")
        df = (
            spark.read.option("header", "true")
            .csv(csv_paths)
            .withColumn("etl_time", to_timestamp("etl_time"))
        )

        delta_path = f"s3://{bucket}/{raw_prefix}/{table_name}/"
        if DeltaTable.isDeltaTable(spark, delta_path):
            print("📥 Appending to existing Delta table...")
            df.write.format("delta").mode("append").save(delta_path)
        else:
            print("🆕 Creating new Delta table...")
            df.write.format("delta").mode("overwrite").save(delta_path)

        print(f"✅ Done processing: {table_name}")

    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")

print("🎉 All tables processed.")
