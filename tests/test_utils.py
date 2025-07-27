from unittest.mock import patch, MagicMock
from datetime import datetime
from openair_api import utils
import pandas as pd
import io
import boto3
# -----------------------------
# load_mapping_csv
# -----------------------------
@patch.object(utils, "s3")
def test_load_mapping_csv(mock_s3):
    csv_content = "email,issuer_company,updated_at\nuser@x.com,Blend LLC,2025-07-01T10:00:00"
    mock_s3.get_object.return_value = {"Body": io.BytesIO(csv_content.encode("utf-8"))}

    df = utils.load_mapping_csv()
    assert df.shape[0] == 1
    assert df.iloc[0]["email"] == "user@x.com"
    assert df.iloc[0]["issuer_company"] == "Blend LLC"

# -----------------------------
# load_mapping_csv - NoSuchKey
# -----------------------------
@patch.object(utils, "s3")
def test_load_mapping_csv_key_not_found(mock_s3):
    mock_s3.exceptions = boto3.client("s3").exceptions  # mimic boto3 exceptions
    mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey({}, "GetObject")

    df = utils.load_mapping_csv()
    assert df.empty
    assert list(df.columns) == ["email", "issuer_company", "updated_at"]

# -----------------------------
# save_mapping_csv
# -----------------------------
@patch.object(utils, "s3")
def test_save_mapping_csv(mock_s3):
    df = pd.DataFrame([
        {"email": "a@x.com", "issuer_company": "Blend", "updated_at": "2025-07-07T00:00:00"}
    ])
    mock_s3.put_object = MagicMock()
    utils.save_mapping_csv(df)

    mock_s3.put_object.assert_called_once()
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == utils.BUCKET
    assert kwargs["Key"] == utils.KEY
    assert "a@x.com" in kwargs["Body"]

# -----------------------------
# get_latest_issuer_for_email
# -----------------------------
def test_get_latest_issuer_for_email():
    df = pd.DataFrame([
        {"email": "john@x.com", "issuer_company": "Company A", "updated_at": "2025-07-07T10:00:00"},
        {"email": "john@x.com", "issuer_company": "Company B", "updated_at": "2025-07-08T10:00:00"}
    ])
    issuer = utils.get_latest_issuer_for_email(df, "JOHN@x.com")
    assert issuer == "Company B"

# -----------------------------
# get_latest_issuer_for_email - no match
# -----------------------------
def test_get_latest_issuer_for_email_not_found():
    df = pd.DataFrame([
        {"email": "someone@x.com", "issuer_company": "X", "updated_at": "2025-07-07T10:00:00"}
    ])
    issuer = utils.get_latest_issuer_for_email(df, "other@x.com")
    assert issuer is None

# -----------------------------
# add_mapping
# -----------------------------
@patch("openair_api.utils.datetime")
def test_add_mapping(mock_datetime):
    mock_datetime.utcnow.return_value = datetime(2025, 7, 9, 12, 0, 0)
    df = pd.DataFrame([
        {"email": "a@x.com", "issuer_company": "X", "updated_at": "2025-07-01T00:00:00"}
    ])
    new_df = utils.add_mapping(df, "b@x.com", "Y")

    assert new_df.shape[0] == 2
    assert "b@x.com" in new_df["email"].values
    assert new_df.iloc[1]["issuer_company"] == "Y"

# -----------------------------
# get_summary
# -----------------------------
@patch("openair_api.utils.get_time_entries")
@patch("openair_api.utils.summarize_billable_hours")
def test_get_summary(mock_summarize, mock_get_entries):
    mock_get_entries.return_value = [{"userId": "1", "decimalHours": 4.0}]
    mock_summarize.return_value = {"1": 4.0}
    summary = utils.get_summary("2025-07-01", "2025-07-31")

    assert summary == {"1": 4.0}
    mock_get_entries.assert_called_once_with("2025-07-01", "2025-07-31", None)
    mock_summarize.assert_called_once()
# -----------------------------
# summarize_billable_hours
# -----------------------------
def test_summarize_billable_hours():
    entries = [
        {"userId": "u1", "decimalHours": 3.5},
        {"userId": "u1", "decimalHours": 1.5},
        {"userId": "u2", "decimalHours": 2.0},
    ]
    result = utils.summarize_billable_hours(entries)
    assert result == {"u1": 5.0, "u2": 2.0}

# -----------------------------
# generate_csv
# -----------------------------
@patch("openair_api.utils.datetime")
def test_generate_csv(mock_datetime):
    mock_datetime.now.return_value = datetime(2025, 7, 7, 15, 0, 0)
    headers = ["col1", "col2", "etl_time"]
    rows = [["a", "b"], ["c", "d"]]

    csv_str = utils.generate_csv(headers, rows)
    lines = csv_str.strip().splitlines()  # 👈 splitlines maneja \n y \r\n

    assert lines[0] == "col1,col2,etl_time"
    assert lines[1].startswith("a,b,2025-07-07T15:00:00")
    assert lines[2].startswith("c,d,2025-07-07T15:00:00")

# -----------------------------
# transform_users_to_csv
# -----------------------------
@patch("openair_api.utils.generate_csv")
def test_transform_users_to_csv(mock_generate_csv):
    users = [{"id": "1", "firstName": "John", "lastName": "Doe", "email": "john@x.com"}]
    utils.transform_users_to_csv(users)
    expected_headers = ["id", "firstName", "lastName", "email", "etl_time"]
    expected_rows = [["1", "John", "Doe", "john@x.com"]]
    mock_generate_csv.assert_called_once_with(expected_headers, expected_rows)

# -----------------------------
# transform_summary_to_csv
# -----------------------------
@patch("openair_api.utils.generate_csv")
def test_transform_summary_to_csv(mock_generate_csv):
    summary = {"123": 5.25}
    utils.transform_summary_to_csv(summary, "2025-07-01", "2025-07-31")
    expected_headers = ["userId", "total_billable_hours", "start_date", "end_date", "etl_time"]
    expected_rows = [["123", "5.25", "2025-07-01", "2025-07-31"]]
    mock_generate_csv.assert_called_once_with(expected_headers, expected_rows)

# -----------------------------
# put_in_s3 (from_dict=True)
# -----------------------------
@patch.object(utils, "s3")
@patch("openair_api.utils.datetime")
def test_put_in_s3_from_dict(mock_datetime, mock_s3):
    # mock_s3 is now the patched utils.s3 client
    mock_datetime.now.return_value = datetime(2025, 7, 7, 10, 0, 0)
    mock_s3.put_object = MagicMock()

    data = {
        "email": "test@x.com",
        "razon_social": "Empresa",
        "ruc_emisor": "123456",
        "amount_of_hours": 10.5,
        "rate": 100.0,
        "description": "Service"
    }

    utils.put_in_s3(
        object=data,
        bucket_name="my-bucket",
        s3_prefix="year=2025/month=07/",
        file_name="test_invoice",
        timestamp_suffix=False,
        from_dict=True
    )

    expected_key = "year=2025/month=07/test_invoice.csv"
    mock_s3.put_object.assert_called_once()
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == "my-bucket"
    assert kwargs["Key"] == expected_key
    assert "test@x.com" in kwargs["Body"]

# -----------------------------
# process_invoice
# -----------------------------
@patch("openair_api.utils.put_in_s3")
@patch("openair_api.utils.add_partition_to_glue")
def test_process_invoice_success(mock_add_partition, mock_put_in_s3):
    invoice = {
        "razon_social": "Empresa SA",
        "ruc_emisor": "12345678",
        "amount_of_hours": 5.5,
        "rate": 120.0,
        "description": "Desarrollo"
    }
    email = "test@blend.com"
    end_date = datetime(2025, 6, 30).date()

    success, err = utils.process_invoice(invoice, email, end_date, bucket_name="my-bucket")
    assert success is True
    assert err is None
    mock_put_in_s3.assert_called_once()
    mock_add_partition.assert_called_once_with(2025, 6, "my-bucket")

# -----------------------------
# add_partition_to_glue
# -----------------------------
@patch("openair_api.utils.boto3.client")
def test_add_partition_to_glue(mock_boto3_client):
    mock_glue = MagicMock()
    mock_boto3_client.return_value = mock_glue

    utils.add_partition_to_glue(2025, 7, "my-bucket")
    mock_glue.batch_create_partition.assert_called_once()
    args, kwargs = mock_glue.batch_create_partition.call_args
    assert kwargs["DatabaseName"] == "openair"
    assert kwargs["TableName"] == "facturas"
    assert "my-bucket" in kwargs["PartitionInputList"][0]["StorageDescriptor"]["Location"]
