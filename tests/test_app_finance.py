from unittest.mock import patch, MagicMock
from app_finance import (
    trigger_step_function,
    wait_for_step_function_completion,
)

@patch("app_finance.boto3.client")
def test_trigger_step_function(mock_boto_client):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client
    mock_client.start_execution.return_value = {
        "executionArn": "arn:mocked-execution"
    }

    result = trigger_step_function()

    mock_client.start_execution.assert_called_once()
    assert result == "arn:mocked-execution"

@patch("app_finance.boto3.client")
@patch("app_finance.st.spinner")  # avoid real spinner
@patch("app_finance.time.sleep", return_value=None)
def test_wait_for_step_function_completion(mock_sleep, mock_spinner, mock_boto_client):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    mock_client.describe_execution.side_effect = [
        {"status": "RUNNING"},
        {"status": "SUCCEEDED"},
    ]

    result = wait_for_step_function_completion("arn:test")

    assert mock_client.describe_execution.call_count == 2
    assert result == "SUCCEEDED"
