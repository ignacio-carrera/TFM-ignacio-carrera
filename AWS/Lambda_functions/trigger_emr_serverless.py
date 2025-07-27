import boto3
import os

emr_serverless = boto3.client("emr-serverless")

# Constants
APPLICATION_ID = os.environ["APPLICATION_ID"]
EXECUTION_ROLE_ARN = os.environ["EXECUTION_ROLE_ARN"]
DELTA_JAR_S3_PATH = os.environ["DELTA_JAR_S3_PATH"]
DELTA_PYTHON_ZIP_S3_PATH = os.environ["DELTA_PYTHON_ZIP_S3_PATH"]

S3_SCRIPT_PATHS = {
    "landing_to_raw": os.environ["SCRIPT_LANDING_TO_RAW"],
    "raw_to_std": os.environ["SCRIPT_RAW_TO_STD"]
}


def lambda_handler(event, context):
    job_name = event.get("job_name")

    if job_name not in S3_SCRIPT_PATHS:
        raise ValueError(f"Unknown job_name: {job_name}")

    entry_point = S3_SCRIPT_PATHS[job_name]

    job_driver = {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "sparkSubmitParameters": (
                "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
                f"--jars {DELTA_JAR_S3_PATH} "
                f"--py-files {DELTA_PYTHON_ZIP_S3_PATH}"
            )
        }
    }

    response = emr_serverless.start_job_run(
        applicationId=APPLICATION_ID,
        executionRoleArn=EXECUTION_ROLE_ARN,
        jobDriver=job_driver,
        configurationOverrides={},  # Add Spark config here if needed
        name=job_name
    )

    job_run_id = response["jobRunId"]
    print(f"Started EMR Serverless job: {job_name}, JobRunId: {job_run_id}")

    return {
        "JobName": job_name,
        "JobRunId": job_run_id,
        "ApplicationId": APPLICATION_ID
    }
