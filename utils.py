from collections import defaultdict
import csv
from datetime import datetime
import boto3
import io
from openair_api.openair_execute import get_time_entries
import pandas as pd

BUCKET = "tfm-bucket-openair"
KEY ='std_layer/email_issuer_mapping/email_issuer_mapping.csv'

s3 = boto3.client("s3")

def load_mapping_csv():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        return df
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame(columns=["email", "issuer_company", "updated_at"])

def save_mapping_csv(df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=csv_buffer.getvalue())

def get_latest_issuer_for_email(df, email):
    match = df[df["email"].str.lower() == email.lower()]
    if match.empty:
        return None
    return match.sort_values("updated_at", ascending=False).iloc[0]["issuer_company"]

def add_mapping(df, email, issuer):
    now = datetime.utcnow().isoformat()
    new_row = pd.DataFrame([{"email": email, "issuer_company": issuer, "updated_at": now}])
    return pd.concat([df, new_row], ignore_index=True)


def summarize_billable_hours(entries):
    user_hours = defaultdict(float)
    for entry in entries:
        user_id = entry.get("userId")
        hours = entry.get("decimalHours", 0)
        user_hours[user_id] += hours
    return dict(user_hours)


def generate_csv(headers, rows):
    etl_time = datetime.now()
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Write header
    writer.writerow(headers)

    # Write rows
    for row in rows:
        writer.writerow(row + [etl_time.isoformat()])

    return csv_buffer.getvalue()

def transform_users_to_csv(users):
    headers = ["id", "firstName", "lastName", "email", "etl_time"]
    rows = [
        [
            user.get("id"),
            user.get("firstName", ""),
            user.get("lastName", ""),
            user.get("email", "")
        ]
        for user in users
    ]
    return generate_csv(headers, rows)

def transform_summary_to_csv(summary, start_date=None, end_date=None):
    headers = ["userId", "total_billable_hours", "start_date", "end_date", "etl_time"]
    rows = [
        [user_id, f"{total_hours:.2f}", start_date, end_date]
        for user_id, total_hours in summary.items()
    ]
    return generate_csv(headers, rows)

def put_in_s3(
    object,
    bucket_name="tfm-bucket-openair",
    s3_prefix="landing/",
    file_name="",
    timestamp_suffix=True,
    as_csv=True,
    from_dict=False
):
    if from_dict:
        buffer = io.StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator='\n')

        row = [
            object.get("email", ""),
            object.get("razon_social", ""),
            object.get("ruc_emisor", ""),
            object.get("amount_of_hours", ""),
            object.get("rate", ""),
            object.get("description", "").replace("\n", " ")
        ]
        writer.writerow(row)
        content = buffer.getvalue()
    else:
        content = object  # already a string

    ext = "csv" if as_csv else "parquet"  # For future flexibility
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H%M%S") if timestamp_suffix else ""
    suffix = f"_{timestamp_str}" if timestamp_suffix else ""
    s3_key = f"{s3_prefix}{file_name}{suffix}.{ext}"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=content)

def process_invoice(json_data: dict, email: str, end_date: datetime.date, bucket_name="facturas-blend-360"):
    """
    Saves invoice data to S3 partitioned by year and month, and updates Glue/Athena partition.
    """
    year = end_date.year
    month = end_date.month

    try:
        # Format S3 path as expected by Athena
        s3_prefix = f"year={year}/month={month:02d}/"
        file_name = f"{email}_{year}_{month:02d}"

        # Upload CSV
        put_in_s3(
            object={
                "email": email,
                "razon_social": json_data["razon_social"],
                "ruc_emisor": json_data["ruc_emisor"],
                "amount_of_hours": json_data["amount_of_hours"],
                "rate": json_data["rate"],
                "description": json_data["description"]
            },
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            file_name=file_name,
            timestamp_suffix=False,
            from_dict=True
        )

        # Register partition in Glue
        add_partition_to_glue(year, month, bucket_name)

        return True, None
    except Exception as e:
        return False, str(e)

def add_partition_to_glue(year: int, month: int, bucket: str):
    glue = boto3.client("glue", region_name="us-east-2")
    s3_path = f"s3://{bucket}/year={year}/month={month:02d}/"

    glue.batch_create_partition(
        DatabaseName="openair",
        TableName="facturas",
        PartitionInputList=[
            {
                'Values': [str(year), str(month)],
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'email', 'Type': 'string'},
                        {'Name': 'razon_social', 'Type': 'string'},
                        {'Name': 'ruc_emisor', 'Type': 'string'},
                        {'Name': 'amount_of_hours', 'Type': 'double'},
                        {'Name': 'rate', 'Type': 'double'},
                        {'Name': 'description', 'Type': 'string'}
                    ],
                    'Location': s3_path,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        'Parameters': {
                            'separatorChar': ',',
                            'quoteChar': '"'
                        }
                    }
                }
            }
        ]
    )

def get_summary(start_date, end_date, user_id = None):
    entries = get_time_entries(start_date, end_date, user_id)
    summary = summarize_billable_hours(entries)
    return summary
