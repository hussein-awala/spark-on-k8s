from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from copy import deepcopy
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Hashable

MAX_SNAPSHOTS = 1024


class _Entry:
    __slots__ = ("expires_at", "task")

    def __init__(self, task: asyncio.Task):
        self.task = task
        # Unset while the lookup is in flight: concurrent viewers share it.
        self.expires_at: float | None = None


class SnapshotCache:
    """Share short-lived read snapshots between concurrent viewers in one process.

    Callers authorize and resolve their own task binding first; the key must identify that
    binding. Failed lookups are not retained, so a later request retries them.
    """

    def __init__(
        self, ttl: float, max_entries: int = MAX_SNAPSHOTS, clock: Callable[[], float] = time.monotonic
    ):
        self.ttl = ttl
        self.max_entries = max_entries
        self._clock = clock
        self._entries: OrderedDict[Hashable, _Entry] = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    async def get(self, key: Hashable, load: Callable[[], Awaitable[Any]]) -> Any:
        if self.ttl <= 0:
            return await load()
        now = self._clock()
        entry = self._entries.get(key)
        if entry is None or (entry.expires_at is not None and entry.expires_at <= now):
            self._discard_expired(now)
            # A separate task keeps one viewer's disconnection from cancelling the shared lookup.
            entry = _Entry(asyncio.ensure_future(load()))
            entry.task.add_done_callback(lambda task, key=key, entry=entry: self._settled(key, entry))
            self._entries[key] = entry
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
        else:
            self._entries.move_to_end(key)
        return deepcopy(await asyncio.shield(entry.task))

    def invalidate(self, key: Hashable) -> None:
        self._entries.pop(key, None)

    def clear(self) -> None:
        for entry in self._entries.values():
            entry.task.cancel()
        self._entries.clear()

    def _settled(self, key: Hashable, entry: _Entry) -> None:
        if entry.task.cancelled() or entry.task.exception() is not None:
            if self._entries.get(key) is entry:
                del self._entries[key]
        else:
            entry.expires_at = self._clock() + self.ttl

    def _discard_expired(self, now: float) -> None:
        for key in [
            key
            for key, entry in self._entries.items()
            if entry.expires_at is not None and entry.expires_at <= now
        ]:
            del self._entries[key]
