import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Create fake exceptions to simulate boto3
class FakeEntityNotFoundException(Exception):
    pass

# Patch boto3.client before importing the module
with patch("boto3.client") as mock_boto3_client:
    mock_glue = MagicMock()
    mock_athena = MagicMock()

    mock_glue.exceptions = MagicMock()
    mock_glue.exceptions.EntityNotFoundException = FakeEntityNotFoundException

    def client_side_effect(service_name, *args, **kwargs):
        if service_name == "glue":
            return mock_glue
        elif service_name == "athena":
            return mock_athena
        else:
            raise ValueError(f"Unknown service: {service_name}")

    mock_boto3_client.side_effect = client_side_effect

    # Import the module after boto3 is patched
    from AWS.Lambda_functions.qa_consistency_check import (
        create_glue_database_if_not_exists,
        create_delta_table_if_not_exists,
        run_athena_query,
        get_athena_query_result_count,
        run_data_consistency_check,
        lambda_handler
    )


class TestQAConsistencyChecks(unittest.TestCase):

    def setUp(self):
        mock_glue.reset_mock()
        mock_athena.reset_mock()

    def test_create_glue_database_exists(self):
        mock_glue.get_database.return_value = {"Database": {"Name": "openair"}}
        create_glue_database_if_not_exists()
        mock_glue.get_database.assert_called_once_with(Name="openair")
        mock_glue.create_database.assert_not_called()

    def test_create_glue_database_missing(self):
        mock_glue.get_database.side_effect = FakeEntityNotFoundException("not found")
        create_glue_database_if_not_exists()
        mock_glue.create_database.assert_called_once_with(DatabaseInput={"Name": "openair"})

    @patch("AWS.Lambda_functions.qa_consistency_check.run_athena_query")
    def test_create_delta_table_missing(self, mock_run_athena_query):
        mock_glue.get_table.side_effect = FakeEntityNotFoundException("not found")
        create_delta_table_if_not_exists("test_table", "s3://some/path")
        mock_run_athena_query.assert_called_once()

    @patch("AWS.Lambda_functions.qa_consistency_check.time.sleep", return_value=None)
    def test_run_athena_query_success(self, _):
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "123"}
        mock_athena.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
        ]
        result = run_athena_query("SELECT 1")
        self.assertEqual(result, "123")

    @patch("AWS.Lambda_functions.qa_consistency_check.run_athena_query", return_value="456")
    def test_get_athena_query_result_count(self, mock_run_query):
        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "count"}]},
                    {"Data": [{"VarCharValue": "42"}]}
                ]
            }
        }
        count = get_athena_query_result_count("SELECT COUNT(*) FROM users")
        self.assertEqual(count, 42)

    @patch("AWS.Lambda_functions.qa_consistency_check.get_athena_query_result_count")
    def test_run_data_consistency_check_all_ok(self, mock_get_count):
        mock_get_count.side_effect = [10, 10, 5, 5]
        result = run_data_consistency_check()
        self.assertIn("✅ Consistency OK for users", result[0])
        self.assertIn("✅ Consistency OK for billable_hours_summary", result[1])

    @patch("AWS.Lambda_functions.qa_consistency_check.create_glue_database_if_not_exists")
    @patch("AWS.Lambda_functions.qa_consistency_check.create_delta_table_if_not_exists")
    @patch("AWS.Lambda_functions.qa_consistency_check.run_data_consistency_check", return_value=["✅ OK"])
    @patch("AWS.Lambda_functions.qa_consistency_check.datetime")
    def test_lambda_handler(self, mock_datetime, mock_check, mock_create_table, mock_create_db):
        mock_datetime.now.return_value.strftime.return_value = "Jul 11, 2025"
        response = lambda_handler({}, {})
        self.assertIn("Jul 11, 2025, ✅ OK", response["log_output"])


if __name__ == "__main__":
    unittest.main()
