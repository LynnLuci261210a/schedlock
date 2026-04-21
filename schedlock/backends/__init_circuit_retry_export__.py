# This stub exists so the feature is self-contained.
# In a real integration the symbol would be added to
# schedlock/backends/__init__.py's __all__ list.
#
# Usage:
#   from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
#
# Example:
#   from schedlock.backends.memory_backend import MemoryBackend
#   from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
#
#   backend = CircuitRetryBackend(
#       MemoryBackend(),
#       retries=3,
#       delay=0.05,
#       failure_threshold=5,
#       reset_timeout=30.0,
#   )
#   acquired = backend.acquire("my-job", "worker-1", ttl=60)
