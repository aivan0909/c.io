import requests
import sys

BASE_URL = "http://127.0.0.1:8000"

def test_endpoint(name, url):
    print(f"Testing {name} ({url})...")
    try:
        response = requests.get(url, timeout=5)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Response Snippet:", str(response.json())[:100])
        else:
            print("  Response:", response.text[:100])
        return response.status_code
    except Exception as e:
        print(f"  Failed: {e}")
        return None

print("=== DEBUG PROBE START ===")

# 1. Test Watchlist (New Feature) - Expect 200 if new code, 404 if old code
status_watchlist = test_endpoint("Watchlist (New Feature)", f"{BASE_URL}/api/watchlist")

# 2. Test Scan (Modified Logging)
status_scan = test_endpoint("Market Scan", f"{BASE_URL}/api/scan?min_price=0&max_price=100")

# 3. Test Analyze (Core Feature)
status_analyze = test_endpoint("Stock Analysis (2330)", f"{BASE_URL}/api/analyze/2330")

print("\n=== DIAGNOSIS ===")
if status_watchlist == 404:
    print("CRITICAL: Watchlist endpoint returned 404.")
    print("CONCLUSION: The server is running OLD CODE. You MUST restart the server.")
elif status_watchlist == 200:
    print("Watchlist endpoint exists. Code seems updated.")
elif status_watchlist is None:
    print("Could not connect to server. Is it running?")
else:
    print(f"Unexpected status for watchlist: {status_watchlist}")

print("=== DEBUG PROBE END ===")
