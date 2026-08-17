# LinkPlease

> [!IMPORTANT]
> **Render Free Tier Deployment:** The application is hosted on Render's free tier. If the service has spun down due to inactivity, it may take 50+ seconds to wake up on the first request.

## 🚀 Deployed System & Live URLs

- **Deployed Swagger UI (API Docs):** [https://linkplease-s85d.onrender.com/docs](https://linkplease-s85d.onrender.com/docs)
- **Simulator Swagger UI:** [https://pseudogram-api.onrender.com/docs](https://pseudogram-api.onrender.com/docs)
- **Live Stats Page:** [https://linkplease-s85d.onrender.com/stats](https://linkplease-s85d.onrender.com/stats)
- **Loom Walkthrough Video:** [https://www.loom.com/share/b2d3dfbf835e477da2fb33e5cb602a5b](https://www.loom.com/share/b2d3dfbf835e477da2fb33e5cb602a5b)

To run the automated tests against this deployed environment, open your terminal next to the browser and run:
```bash
python test_deployed.py
```

## 📝 Submission Metadata

```json
{
  "email": "pandey_ujjawal@srmap.edu.in",
  "api_key": "dXp3YWxwYW5kZXlAZ21haWwuY29t.1db5e4bc825d037cb2d1",
  "github_repo": "https://github.com/ujjwalpan001/linkplease",
  "working_url": "https://linkplease-s85d.onrender.com",
  "loom_url": "https://www.loom.com/share/b2d3dfbf835e477da2fb33e5cb602a5b",
  "parts_completed": "A+B+C",
  "start_date": "5th august 2026"
}
```

---

Automates Instagram DMs: when a comment matches a keyword, the commenter gets a DM — exactly once.

Built for the LinkPlease intern assignment. Covers Parts A + B + C.

---

## Stack

- **FastAPI** + **Uvicorn** — async HTTP, returns 200 from `/webhook` in < 5 ms
- **SQLite** (WAL mode) — persistent; survives process restarts
- **Two daemon threads** — DM worker + delivery reconciler

---

## Quick start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Register and get API key (one time)
python register.py

# 3. Run
uvicorn app:app --reload --port 8000
```

---

## API

### `POST /rules`
```bash
curl -X POST http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword": "PRICE", "dm_message": "Here is the price list!"}'
```

### `POST /webhook`
Receives events from PseudoGram. Signature-verified. Returns 200 immediately.

### `GET /stats`
```json
{ "sent": 12, "failed": 1, "queued": 3, "duplicates_blocked": 45 }
```

---

## Architecture

```
POST /webhook
  └─ verify HMAC signature (Part B)
  └─ return 200 immediately
  └─ background task:
       ├─ deduplicate event_id (seen_events table)
       ├─ match text against rules (case-insensitive)
       └─ INSERT into dm_queue with UNIQUE(rule_id:user_id) — dedup gate

DM Worker thread (daemon)
  └─ polls dm_queue for pending rows
  └─ token bucket: 10 calls / 60 s
  └─ exponential backoff on 500 (2^n seconds, max 6 attempts)
  └─ respects Retry-After on 429
  └─ sets Idempotency-Key header on every send

Reconciler thread (daemon, every 30 s)  [Part C]
  └─ GET /v1/dm/{dm_id} for all accepted DMs
  └─ delivered → sent | failed → re-queue or final-fail
```

---

## Deployment (Render / Railway)

Set environment variable:
```
PSEUDOGRAM_API_KEY=<your key>
DB_PATH=/data/linkplease.db   # mount a persistent volume at /data
```

Start command: `uvicorn app:app --host 0.0.0.0 --port 8000`

Or use the included `Dockerfile`.
