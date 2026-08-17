"""
Quick test: sends a properly HMAC-signed webhook to the deployed app.
If you get 200, signature verification is working.
If you get 403, there's a bug in the HMAC verification.
"""
import hashlib
import hmac
import json
import requests

DEPLOYED_URL = "https://linkplease-s85d.onrender.com"
API_KEY = "dXp3YWxwYW5kZXlAZ21haWwuY29t.1db5e4bc825d037cb2d1"

payload = {
    "event_id": "sig-test-001",
    "event_type": "comment.created",
    "data": {
        "comment_id": "cmt-sig-001",
        "text": "send me the link please",
        "from": {"user_id": "usr_test_123"}
    }
}

# Compute HMAC exactly as PseudoGram does
raw_body = json.dumps(payload, separators=(',', ':')).encode()
signature = "sha256=" + hmac.new(
    API_KEY.encode(), raw_body, hashlib.sha256
).hexdigest()

print(f"Payload:   {raw_body.decode()}")
print(f"Signature: {signature}")
print(f"\nSending to {DEPLOYED_URL}/webhook ...")

r = requests.post(
    f"{DEPLOYED_URL}/webhook",
    data=raw_body,
    headers={
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": signature,
    }
)

print(f"\nStatus:  {r.status_code}")
print(f"Body:    {r.text}")

if r.status_code == 200:
    print("\n✅ Signature verification is WORKING!")
elif r.status_code == 403:
    print("\n❌ Got 403 - HMAC mismatch. The key or computation is wrong.")
elif r.status_code == 422:
    print("\n❌ Got 422 - Body schema rejected by FastAPI (Pydantic issue).")
else:
    print(f"\n❌ Unexpected status {r.status_code}")
