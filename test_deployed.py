"""
LinkPlease - End-to-End Test Script
Run: python test_deployed.py
"""

import requests
import time
import json

DEPLOYED_URL = "https://linkplease-s85d.onrender.com"
API_KEY = "dXp3YWxwYW5kZXlAZ21haWwuY29t.1db5e4bc825d037cb2d1"
PSEUDOGRAM_URL = "https://pseudogram-api.onrender.com"

headers_api = {"X-API-Key": API_KEY}

print("=" * 60)
print("LinkPlease Deployment Test")
print("=" * 60)

# Step 1: Health check
print("\n[1] Checking /stats (health check)...")
r = requests.get(f"{DEPLOYED_URL}/stats")
print(f"    Status: {r.status_code}")
print(f"    Body:   {r.json()}")
assert r.status_code == 200, "FAILED: /stats not working!"
print("    OK - App is live!")

# Step 2: Create rules for multiple common keywords
print("\n[2] Creating rules for common keywords...")
keywords = [
    ("link",  "Here is the link you asked for!"),
    ("price", "Here is the price list!"),
    ("info",  "Here is more info for you!"),
    ("buy",   "Here is how to buy!"),
    ("send",  "Sending you the details now!"),
]
for kw, msg in keywords:
    r = requests.post(f"{DEPLOYED_URL}/rules", json={"keyword": kw, "dm_message": msg})
    if r.status_code == 201:
        print(f"    OK - Rule created: keyword='{kw}'")
    else:
        print(f"    WARN - Rule '{kw}' failed: {r.status_code} {r.text}")

# Step 3: Start 500-event load test
print("\n[3] Starting 500-event load test (10 seconds)...")
r = requests.post(
    f"{PSEUDOGRAM_URL}/v1/simulate/start",
    headers={**headers_api, "Content-Type": "application/json"},
    json={
        "webhook_url": f"{DEPLOYED_URL}/webhook",
        "count": 500,
        "duration_seconds": 10
    }
)
print(f"    Status: {r.status_code}")
print(f"    Body:   {r.json()}")

if r.status_code != 200:
    print("FAILED: Simulate start failed!")
    exit(1)

run_id = r.json().get("run_id")
print(f"    OK - Load test started! run_id = {run_id}")

# Step 4: Wait for events + worker to process
print("\n[4] Waiting 3 minutes for events to process...")
for i in range(18):
    time.sleep(10)
    stats = requests.get(f"{DEPLOYED_URL}/stats").json()
    print(f"    [{(i+1)*10}s] {stats}")
    if i > 6 and stats.get("queued", 0) == 0:
        print("    Queue drained!")
        break

# Step 5: Get ground truth
print(f"\n[5] Fetching ground truth (run_id={run_id})...")
r = requests.get(
    f"{PSEUDOGRAM_URL}/v1/simulate/{run_id}/truth",
    headers=headers_api
)
print(f"    Status: {r.status_code}")

if r.status_code != 200:
    print(f"FAILED: {r.text}")
    exit(1)

truth = r.json()

# Step 6: Final comparison
print("\n[6] Final comparison...")
stats = requests.get(f"{DEPLOYED_URL}/stats").json()

print("\n" + "=" * 60)
print("RESULTS")
print("=" * 60)
print(f"  Your /stats:")
print(f"    sent               = {stats.get('sent')}")
print(f"    failed             = {stats.get('failed')}")
print(f"    queued             = {stats.get('queued')}")
print(f"    duplicates_blocked = {stats.get('duplicates_blocked')}")
print(f"\n  Ground Truth (what PseudoGram sent):")
for k, v in truth.items():
    print(f"    {k} = {v}")
print("=" * 60)
