from schedlock.backends.base import BaseBackend


class LabeledBackend(BaseBackend):
    """Wraps a backend and attaches arbitrary metadata labels."""

    def __init__(self, inner: BaseBackend, labels: dict):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(labels, dict):
            raise TypeError("labels must be a dict")
        if not labels:
            raise ValueError("labels must not be empty")
        for k, v in labels.items():
            if not isinstance(k, str) or not k:
                raise ValueError("label keys must be non-empty strings")
            if not isinstance(v, str):
                raise ValueError("label values must be strings")
        self._inner = inner
        self._labels = dict(labels)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def labels(self) -> dict:
        return dict(self._labels)

    def get_label(self, key: str):
        return self._labels.get(key)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
