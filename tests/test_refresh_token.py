import unittest
from unittest.mock import patch, MagicMock
from access_openair.refresh_token import refresh_access_token  # adjust import if needed

class TestRefreshAccessToken(unittest.TestCase):

    @patch("access_openair.refresh_token.save_tokens")
    @patch("access_openair.refresh_token.requests.post")
    @patch("access_openair.refresh_token.load_tokens")
    def test_successful_token_refresh(self, mock_load_tokens, mock_requests_post, mock_save_tokens):
        # Mock the tokens returned by load_tokens
        mock_load_tokens.return_value = {
            "refresh_token": "mocked_refresh_token"
        }

        # Mock response from requests.post
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "expires_in": 3600
        }
        mock_requests_post.return_value = mock_response

        # Run the function
        token = refresh_access_token()

        # Assert access token was returned
        self.assertEqual(token, "new_access_token")

        # Assert save_tokens was called
        mock_save_tokens.assert_called_once()
        self.assertIn("new_access_token", mock_save_tokens.call_args[0])

    @patch("access_openair.refresh_token.requests.post")
    @patch("access_openair.refresh_token.load_tokens")
    def test_token_refresh_failure(self, mock_load_tokens, mock_requests_post):
        # Load tokens as usual
        mock_load_tokens.return_value = {"refresh_token": "invalid_token"}

        # Simulate a failed refresh
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.text = "Invalid refresh token"
        mock_requests_post.return_value = mock_response

        # Run function
        token = refresh_access_token()

        # Expect None if failed
        self.assertIsNone(token)

    @patch("access_openair.refresh_token.load_tokens")
    def test_missing_tokens(self, mock_load_tokens):
        # Simulate no tokens on disk
        mock_load_tokens.return_value = None

        token = refresh_access_token()

        self.assertIsNone(token)

if __name__ == "__main__":
    unittest.main()
