import json
import time
import os

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "tokens.json")

def save_tokens(access_token, refresh_token, expires_in, expires_at=None):
    if not expires_at:
        expires_at = int(time.time()) + expires_in
    with open(TOKEN_FILE, "w") as f:
        json.dump({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in,
            "expires_at": expires_at
        }, f)

def load_tokens():
    try:
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def is_token_expired():
    tokens = load_tokens()
    if not tokens or "expires_at" not in tokens:
        return True
    return time.time() > tokens["expires_at"]