import requests
from access_openair.token_store import load_tokens, is_token_expired
from access_openair.refresh_token import refresh_access_token

api_key = "SJTZWRmQc3FE726EmgU5"
namespace = "default"
base_url = "https://7050007-blend360-llc.app.sandbox.netsuitesuiteprojectspro.com/rest/v1"

def get_access_token():
    tokens = load_tokens()
    if not tokens:
        return refresh_access_token()
    if is_token_expired():
        return refresh_access_token()
    return tokens["access_token"]


def get_users(limit=1000):
    token = get_access_token()
    if not token:
        print("❌ No valid token available.")
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-OpenAir-API-Key": api_key,
        "X-OpenAir-Namespace": namespace,
        "Content-Type": "application/json"
    }

    all_users = []
    offset = 0

    print("📦 Fetching users from /users...")
    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(f"{base_url}/users", headers=headers, params=params)

        if not response.ok:
            print("❌ Failed to fetch users:")
            print("Status:", response.status_code)
            print("Response:", response.text)
            break

        users = response.json().get("data", [])
        all_users.extend(users)

        meta = response.json().get("meta", {})
        total_rows = meta.get("totalRows", 0)
        offset += limit
        if offset >= total_rows:
            break

    print(f"✅ Total users fetched: {len(all_users)}")
    return all_users


def get_user_id(email: str):
    """
    Given a valid email, return the corresponding user ID.
    """
    all_users = get_users()
    for user in all_users:
        if user.get("email", "").lower() == email.lower():
            return user.get("id")

    print(f"❌ No user found with email: {email}")
    return None



def get_billable_project_ids(limit=1000):
    """Fetch project-tasks and return a set of projectIds where isNonBillable == 0"""
    print("📦 Fetching billable project-task entries from /project-tasks...")
    token = get_access_token()
    if not token:
        print("❌ No valid token available.")
        return set()

    headers = {
        "Authorization": f"Bearer {token}",
        "X-OpenAir-API-Key": api_key,
        "X-OpenAir-Namespace": namespace,
        "Content-Type": "application/json"
    }

    url = f"{base_url}/project-tasks"
    offset = 0
    billable_project_ids = set()

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(url, headers=headers, params=params)

        if not response.ok:
            print("❌ Failed to fetch project-tasks:")
            print("Status:", response.status_code)
            print("Response:", response.text)
            break

        data = response.json().get("data", [])
        for task in data:
            if task.get("isNonBillable") == 0 or task.get("isNonBillable") == None:
                project_id = task.get("projectId")
                if project_id:
                    billable_project_ids.add(project_id)

        meta = response.json().get("meta", {})
        total_rows = meta.get("totalRows", 0)
        offset += limit
        if offset >= total_rows:
            break

    print(f"✅ Total billable project IDs found: {len(billable_project_ids)}")
    return billable_project_ids

def get_time_entries(start_date, end_date, user_id=None, limit=1000):
    token = get_access_token()
    if not token:
        print("❌ No valid token available.")
        return []

    billable_project_ids = get_billable_project_ids(limit=limit)
    if not billable_project_ids:
        print("⚠️ No billable projects found.")
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-OpenAir-API-Key": api_key,
        "X-OpenAir-Namespace": namespace,
        "Content-Type": "application/json"
    }

    all_entries = []
    offset = 0
    while True:
        filters = [
            f'date ON_OR_AFTER "{start_date}"',
            f'date ON_OR_BEFORE "{end_date}"'
        ]
        if user_id:
            filters.append(f'userId EQUAL {user_id}')

        q = " AND ".join(filters)
        params = {
            "q": q,
            "limit": limit,
            "offset": offset
        }

        print(f"🔍 Fetching time entries... offset={offset}")
        response = requests.get(f"{base_url}/time-entries", headers=headers, params=params)

        if not response.ok:
            print("❌ Failed to fetch time entries:")
            print("Status:", response.status_code)
            print("Response:", response.text)
            return []

        entries = response.json().get("data", [])
        # ✅ Keep only time entries linked to billable projectId
        filtered_entries = [e for e in entries if e.get("projectId") in billable_project_ids]
        for e in filtered_entries:
            print(f"✅ Entry {e.get('id')} kept (projectId={e.get('projectId')})")
        all_entries.extend(filtered_entries)

        meta = response.json().get("meta", {})
        total_rows = meta.get("totalRows", 0)
        offset += limit
        if offset >= total_rows:
            break

    return all_entries
