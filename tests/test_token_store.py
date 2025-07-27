import unittest
from unittest.mock import mock_open, patch, MagicMock
import json
import time
from access_openair import token_store

class TestTokenStore(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_save_tokens(self, mock_json_dump, mock_file):
        token_store.save_tokens("access123", "refresh123", 3600, 9999999999)
        mock_file.assert_called_once_with(token_store.TOKEN_FILE, "w")
        mock_json_dump.assert_called_once_with({
            "access_token": "access123",
            "refresh_token": "refresh123",
            "expires_in": 3600,
            "expires_at": 9999999999
        }, mock_file())

    @patch("builtins.open", new_callable=mock_open, read_data='{"access_token": "access123"}')
    def test_load_tokens_success(self, mock_file):
        result = token_store.load_tokens()
        self.assertEqual(result, {"access_token": "access123"})

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_tokens_file_not_found(self, mock_file):
        result = token_store.load_tokens()
        self.assertIsNone(result)

    @patch("access_openair.token_store.load_tokens")
    @patch("time.time", return_value=2000)
    def test_is_token_expired_true(self, mock_time, mock_load):
        # Simulate token expired at 1000
        mock_load.return_value = {"expires_at": 1000}
        self.assertTrue(token_store.is_token_expired())

    @patch("access_openair.token_store.load_tokens")
    @patch("time.time", return_value=500)
    def test_is_token_expired_false(self, mock_time, mock_load):
        # Simulate token expires at 1000
        mock_load.return_value = {"expires_at": 1000}
        self.assertFalse(token_store.is_token_expired())

    @patch("access_openair.token_store.load_tokens", return_value=None)
    def test_is_token_expired_missing_tokens(self, mock_load):
        self.assertTrue(token_store.is_token_expired())

    @patch("access_openair.token_store.load_tokens", return_value={"access_token": "a"})
    def test_is_token_expired_missing_expiry(self, mock_load):
        self.assertTrue(token_store.is_token_expired())

if __name__ == "__main__":
    unittest.main()
