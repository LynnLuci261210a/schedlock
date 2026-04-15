# schedlock

A lightweight Python library for distributed cron-style job locking using Redis or file backends.

---

## Installation

```bash
pip install schedlock
```

---

## Usage

```python
from schedlock import SchedLock

# Using Redis backend
lock = SchedLock(backend="redis", url="redis://localhost:6379")

with lock.acquire("daily-report", ttl=3600):
    # Only one instance will run this block at a time
    generate_daily_report()

# Using file backend (no external dependencies)
lock = SchedLock(backend="file", path="/tmp/schedlock")

with lock.acquire("cleanup-job", ttl=1800):
    run_cleanup()
```

You can also use it as a decorator:

```python
@lock.job(name="sync-users", ttl=600)
def sync_users():
    # Skipped automatically if another instance holds the lock
    pull_and_sync_users()
```

---

## Backends

| Backend | Dependency | Best For |
|---------|------------|----------|
| `redis` | `redis-py` | Distributed multi-host environments |
| `file` | None | Single-host or local development |

---

## Install with Redis support

```bash
pip install schedlock[redis]
```

---

## License

MIT © schedlock contributors