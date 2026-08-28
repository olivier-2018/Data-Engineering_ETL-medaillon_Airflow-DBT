"""Shared timer-heap primitive for deterministic delayed actions (invoice
due-dates/reminders, truck loading duration, account-verification delay) -
see redesign plan §3b. Genuinely random per-tick checks (cancellation
probability, attribute-update rate) don't use this; they stay as plain
random() checks in the tick loop, same as pre-redesign."""
from __future__ import annotations

import heapq
import itertools
import time
from typing import Callable


class Scheduler:
    def __init__(self) -> None:
        self._heap: list[tuple[float, int, Callable[[], None]]] = []
        self._counter = itertools.count()

    def schedule(self, delay_seconds: float, callback: Callable[[], None]) -> None:
        fire_at = time.monotonic() + delay_seconds
        heapq.heappush(self._heap, (fire_at, next(self._counter), callback))

    def tick(self) -> None:
        now = time.monotonic()
        while self._heap and self._heap[0][0] <= now:
            _, _, callback = heapq.heappop(self._heap)
            callback()
