"""
Burst test: sends 30 signed webhooks concurrently to the deployed app.
This simulates what PseudoGram's simulator does.
Run: python test_burst.py
"""
import requests, threading, json, hmac, hashlib, time

URL = "https://linkplease-s85d.onrender.com/webhook"
KEY = "dXp3YWxwYW5kZXlAZ21haWwuY29t.1db5e4bc825d037cb2d1"
results = []

def send(i):
    body = json.dumps({
        "event_id": f"burst-{i}",
        "event_type": "comment.created",
        "data": {
            "comment_id": f"cmt-{i}",
            "text": "send me the link please",
            "from": {"user_id": f"usr_{i}"}
        }
    }, separators=(',', ':')).encode()
    sig = "sha256=" + hmac.new(KEY.encode(), body, hashlib.sha256).hexdigest()
    try:
        r = requests.post(URL, data=body, headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": sig
        }, timeout=10)
        results.append(r.status_code)
        print(f"  [{i:02d}] {r.status_code} {r.text[:50]}")
    except Exception as e:
        results.append(0)
        print(f"  [{i:02d}] TIMEOUT/ERROR: {e}")

print("Sending 30 concurrent signed webhooks to Render...")
threads = [threading.Thread(target=send, args=(i,)) for i in range(30)]
start = time.time()
[t.start() for t in threads]
[t.join() for t in threads]
elapsed = time.time() - start

print(f"\nDone in {elapsed:.1f}s")
print(f"Results: { {s: results.count(s) for s in set(results)} }")

# Check stats after
import time as t
t.sleep(2)
stats = requests.get("https://linkplease-s85d.onrender.com/stats").json()
print(f"Stats after burst: {stats}")
