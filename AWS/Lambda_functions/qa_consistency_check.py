import boto3
import time
from datetime import datetime

# --- Config ---
ATHENA_OUTPUT = "s3://aws-glue-assets-376465608000-us-east-2/athena_results/"
DATABASE_NAME = "openair"
RAW_PATHS = {
    "users_raw": "s3://tfm-bucket-openair/raw_layer/users/",
    "billable_hours_summary_raw": "s3://tfm-bucket-openair/raw_layer/billable_hours_summary/"
}
STD_PATHS = {
    "users_std": "s3://tfm-bucket-openair/std_layer/users/",
    "billable_hours_summary_std": "s3://tfm-bucket-openair/std_layer/billable_hours_summary/"
}

# --- Clients ---
glue = boto3.client("glue")
athena = boto3.client("athena")

# --- Glue Helpers ---
def create_glue_database_if_not_exists():
    try:
        glue.get_database(Name=DATABASE_NAME)
        print("✅ Glue database already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": DATABASE_NAME})
        print("✅ Created Glue database.")

def create_delta_table_if_not_exists(table_name, s3_path):
    try:
        glue.get_table(DatabaseName=DATABASE_NAME, Name=table_name)
        print(f"✅ Glue table '{table_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        query = f"""
        CREATE EXTERNAL TABLE {DATABASE_NAME}.{table_name}
        LOCATION '{s3_path}'
        TBLPROPERTIES ('table_type' = 'DELTA')
        """
        print(f"⚙️ Creating table: {table_name}")
        run_athena_query(query)

# --- Athena Helpers ---
def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE_NAME},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if status != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {status}")
    return query_execution_id

def get_athena_query_result_count(query):
    execution_id = run_athena_query(query)
    results = athena.get_query_results(QueryExecutionId=execution_id)
    rows = results.get("ResultSet", {}).get("Rows", [])
    if len(rows) > 1:
        return int(rows[1]['Data'][0]['VarCharValue'])
    return 0

# --- QA Check ---
def build_distinct_query(table, columns, layer):
    cols_expr = (
        f"ROW({', '.join(columns)})"
        if len(columns) > 1 else columns[0]
    )
    return f"SELECT COUNT(DISTINCT {cols_expr}) FROM {DATABASE_NAME}.{table}_{layer}"

def run_data_consistency_check():
    summary_lines = []
    checks = [
        {
            "label": "users",
            "table": "users",
            "columns": ["id"]
        },
        {
            "label": "billable_hours_summary",
            "table": "billable_hours_summary",
            "columns": ["userid"]
        }
    ]

    for check in checks:
        try:
            raw_query = build_distinct_query(check["table"], check["columns"], "raw")
            std_query = build_distinct_query(check["table"], check["columns"], "std")

            raw_count = get_athena_query_result_count(raw_query)
            std_count = get_athena_query_result_count(std_query)

            if std_count < raw_count:
                summary_lines.append(f"❌ MISMATCH for {check['label']}: Raw: {raw_count} | Std: {std_count}")
            else:
                summary_lines.append(f"✅ Consistency OK for {check['label']}: Raw: {raw_count} | Std: {std_count}")
        except Exception as e:
            summary_lines.append(f"❌ Error in {check['label']}: {str(e)}")

    return summary_lines

# --- Lambda Entry Point ---
def lambda_handler(event, context):
    create_glue_database_if_not_exists()

    for table, path in {**RAW_PATHS, **STD_PATHS}.items():
        create_delta_table_if_not_exists(table, path)

    summary = run_data_consistency_check()
    timestamp = datetime.now().strftime("%b %d, %Y")
    log_output = f"{timestamp}, " + ", ".join(summary)

    return {
        "log_output": log_output
    }
