import base64
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests
import time
from token_store import save_tokens
from dotenv import load_dotenv
import os

load_dotenv("openair_config.env")

# Update with your real credentials
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
redirect_uri = os.getenv("redirect_uri")
sandbox_domain = os.getenv("sandbox_domain")
scope = os.getenv("scope")
state = os.getenv("state")

# Step 1: Open browser for user login
params = {
    "response_type": "code",
    "client_id": client_id,
    "redirect_uri": redirect_uri,
    "scope": scope,
    "state": state
}
auth_url = f"https://{sandbox_domain}/login/oauth2/v1/authorize?" + urllib.parse.urlencode(params)
print("\n[1] Opening browser to authorize application (use your admin account)...\n")
webbrowser.open(auth_url)

# Step 2: Local server to capture redirect with auth code
authorization_code = None

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global authorization_code
        parsed_path = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed_path.query)
        if "code" in query:
            authorization_code = query["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authorization successful! You can close this window.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Authorization failed or cancelled.")

print("[2] Waiting for redirect with authorization code...")
httpd = HTTPServer(("localhost", 3000), OAuthHandler)
httpd.handle_request()

if not authorization_code:
    print("❌ Authorization failed. No code received.")
    exit()

print(f"\n✅ Authorization code received: {authorization_code}\n")

# Step 3: Exchange auth code for tokens
token_url = f"https://{sandbox_domain}/login/oauth2/v1/token"
auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
headers = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "authorization_code",
    "code": authorization_code,
    "redirect_uri": redirect_uri
}

print("[3] Exchanging authorization code for access + refresh tokens...")
response = requests.post(token_url, headers=headers, data=data)

if response.ok:
    tokens = response.json()
    expires_at = int(time.time()) + tokens["expires_in"]
    save_tokens(tokens["access_token"], tokens["refresh_token"], tokens["expires_in"], expires_at)
    print("Tokens received successfully:")
    print("Access Token:\n", tokens["access_token"])
    print("\nRefresh Token:\n", tokens["refresh_token"])
    print("\nExpires In (seconds):", tokens["expires_in"])
else:
    print("Failed to get access token:")
    print("Status:", response.status_code)
    print("Response:", response.text)