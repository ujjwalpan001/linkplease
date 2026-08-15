# FAILURES.md

Every way this system can still lose a DM, send a duplicate, or report a wrong number.

---

## 1. Process restart while a DM is `accepted` but reconciler hasn't run

**Condition:** A DM was accepted (202) and marked `accepted` in the DB, but the process restarts before the 30-second reconciler fires. On restart, the DM remains `accepted` in SQLite and the reconciler will pick it up within 30 seconds. However, if the mock API's `dm_id` expires before the reconciler runs (undefined by the spec), the DM would be permanently lost with `accepted` status — never reaching `sent` or `failed`.

**Likelihood:** Low. Only during restarts. SQLite persistence means no data is lost across restarts; only the timing window of reconciliation is affected.

---

## 2. SQLite write timeout under extreme concurrency (500 events / 10 s)

**Condition:** 500 webhook events arrive in 10 seconds, each spawning a FastAPI background task. SQLite WAL mode allows concurrent reads but serialises writes. With a `busy_timeout` of 5 s, tasks that cannot acquire the write lock within 5 s will raise an exception. The 200 has already been returned; the event is dropped from our processing. The mock API's ~8% re-delivery partially compensates, but is not guaranteed.

**Likelihood:** Low in practice (WAL is fast), but theoretically possible under the worst burst. The correct fix is a proper job queue (Redis/Postgres) rather than SQLite write contention.

---

## 3. `comment.deleted` arriving after the DM is `accepted`

**Condition:** A `comment.deleted` event arrives after the DM has already been sent to the API (status `accepted`). We cancel pending DMs but cannot recall an in-flight one. The DM will be delivered (or fail naturally). There is no mechanism to suppress delivery after a 202 response.

**Likelihood:** ~8% of the time for comments that get deleted (the race window is small but real).

---

## 4. `duplicates_blocked` counter could be undercounted under concurrent inserts

**Condition:** Two threads both fail the `INSERT INTO dm_queue` with `IntegrityError` (same idempotency_key) at nearly the same time. Each then increments the counter. This is actually correct. However, if the counter increment itself is interrupted (e.g., process crash between the `IntegrityError` catch and the `UPDATE counters`), the duplicate is blocked but not counted.

**Likelihood:** Very rare (would require a crash in a sub-millisecond window). Counter could be off by 1–2 per crash event, not cumulative.

---

## 5. Token bucket resets on process restart

**Condition:** The token bucket is in-memory. If the process restarts with 3 tokens remaining (7 calls made in the current window), the bucket resets to 10 tokens. This could briefly allow up to 17 calls before the next window ends, potentially breaching the rolling rate limit.

**Likelihood:** Only on restart during an active burst. The API's 429 response acts as a corrective backstop.
