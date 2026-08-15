#!/usr/bin/env python3
"""
One-shot script: apply for API key, then fetch it.
Run once. Writes PSEUDOGRAM_API_KEY to .env automatically.

Usage:
    python register.py
"""

import json
import sys
import requests

BASE = "https://pseudogram-api.onrender.com"


def apply(name: str, email: str, phone: str, linkedin: str) -> bool:
    resp = requests.post(
        f"{BASE}/v1/apply",
        json={"name": name, "email": email, "phone": phone, "linkedin_url": linkedin},
        timeout=15,
    )
    print(f"[apply] {resp.status_code}: {resp.text}")
    return resp.status_code in (200, 201)


def keygen(email: str) -> str | None:
    resp = requests.post(f"{BASE}/v1/keygen", json={"email": email}, timeout=15)
    print(f"[keygen] {resp.status_code}: {resp.text}")
    if resp.status_code == 200:
        return resp.json().get("api_key")
    return None


def main():
    print("=== PseudoGram Registration ===\n")
    name = input("Full name : ").strip()
    email = input("Email     : ").strip()
    phone = input("Phone     : ").strip()
    linkedin = input("LinkedIn  : ").strip()

    print("\n→ Applying...")
    apply(name, email, phone, linkedin)

    print("\n→ Fetching API key...")
    key = keygen(email)

    if key:
        with open(".env", "w") as f:
            f.write(f"PSEUDOGRAM_API_KEY={key}\n")
            f.write("DB_PATH=linkplease.db\n")
        print(f"\n✅  Key written to .env: {key}")
    else:
        print("\n❌  keygen returned 403 — wait a few seconds and run again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
