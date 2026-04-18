from __future__ import annotations
from typing import Callable, Optional
from schedlock.backends.base import BaseBackend


class ScoringBackend(BaseBackend):
    """Wraps a backend and only allows acquire when a score function returns >= min_score."""

    def __init__(
        self,
        inner: BaseBackend,
        score_fn: Callable[[str, str], float],
        min_score: float = 0.5,
    ) -> None:
        if inner is None:
            raise ValueError("inner backend is required")
        if not callable(score_fn):
            raise TypeError("score_fn must be callable")
        if not isinstance(min_score, (int, float)):
            raise TypeError("min_score must be a number")
        self._inner = inner
        self._score_fn = score_fn
        self._min_score = min_score
        self._scores: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def min_score(self) -> float:
        return self._min_score

    def last_score(self, key: str) -> Optional[float]:
        return self._scores.get(key)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        score = self._score_fn(key, owner)
        self._scores[key] = score
        if score < self._min_score:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
