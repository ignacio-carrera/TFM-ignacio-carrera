from unittest.mock import patch, Mock
from openair_api.openair_execute import (
    get_users,
    get_billable_project_ids,
    get_time_entries,
    get_user_id
)

@patch("openair_api.openair_execute.get_users")
def test_get_user_id_found(mock_get_users):
    mock_get_users.return_value = [
        {"id": "123", "email": "alice@example.com"},
        {"id": "456", "email": "bob@example.com"}
    ]
    result = get_user_id("bob@example.com")
    assert result == "456"

@patch("openair_api.openair_execute.get_users")
def test_get_user_id_not_found(mock_get_users):
    mock_get_users.return_value = [
        {"id": "123", "email": "alice@example.com"}
    ]
    result = get_user_id("nonexistent@example.com")
    assert result is None

# ------------------------
# Test: get_users (with pagination)
# ------------------------
@patch("openair_api.openair_execute.get_access_token", return_value="fake_token")
@patch("openair_api.openair_execute.requests.get")
def test_get_users(mock_get, mock_token):
    # Simulate two paginated calls to /users
    mock_get.side_effect = [
        Mock(ok=True, json=lambda: {
            "data": [{"id": 1, "firstName": "Alice"}],
            "meta": {"totalRows": 2}
        }),
        Mock(ok=True, json=lambda: {
            "data": [{"id": 2, "firstName": "Bob"}],
            "meta": {"totalRows": 2}
        })
    ]

    users = get_users(limit=1)
    assert len(users) == 2
    assert users[0]["id"] == 1
    assert users[1]["id"] == 2

# ------------------------
# Test: get_billable_project_ids
# ------------------------
@patch("openair_api.openair_execute.get_access_token", return_value="fake_token")
@patch("openair_api.openair_execute.requests.get")
def test_get_billable_project_ids(mock_get, mock_token):
    mock_get.return_value.ok = True
    mock_get.return_value.json.return_value = {
        "data": [
            {"projectId": 10, "isNonBillable": 0},
            {"projectId": 11, "isNonBillable": 1},
            {"projectId": 12, "isNonBillable": 0},
        ],
        "meta": {"totalRows": 3}
    }

    result = get_billable_project_ids(limit=3)
    assert result == {10, 12}
    assert mock_get.called

# ------------------------
# Test: get_time_entries with filtering by billable project
# ------------------------
@patch("openair_api.openair_execute.get_access_token", return_value="fake_token")
@patch("openair_api.openair_execute.get_billable_project_ids", return_value={1001, 1003})
@patch("openair_api.openair_execute.requests.get")
def test_get_time_entries(mock_get, mock_billable, mock_token):
    mock_get.return_value.ok = True
    mock_get.return_value.json.return_value = {
        "data": [
            {"id": "a", "projectId": 1001},
            {"id": "b", "projectId": 9999},   # should be filtered out
            {"id": "c", "projectId": 1003}
        ],
        "meta": {"totalRows": 3}
    }

    result = get_time_entries("2025-06-01", "2025-06-25", user_id="123", limit=10)
    assert len(result) == 2
    assert result[0]["id"] == "a"
    assert result[1]["id"] == "c"
    assert mock_billable.called
