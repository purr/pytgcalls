import asyncio
from typing import Dict

from .wait_counter_lock import WaitCounterLock


class ChatLock:
    def __init__(self):
        self._main_lock = asyncio.Lock()
        self._chat_lock: Dict[int, WaitCounterLock] = {}

    async def _remove_callback(self, chat_id: int):
        async with self._main_lock:
            # Defensive: a concurrent ``acquire`` + drained ``__aexit__``
            # pair could remove the entry between this callback's
            # registration and execution.  Use ``.get`` so we never raise
            # KeyError on a benign race.  Mirrors pytgcalls upstream
            # PR #319.
            lock = self._chat_lock.get(chat_id)
            if lock and not lock.waiters():
                self._chat_lock.pop(chat_id, None)

    async def acquire(self, chat_id: int) -> WaitCounterLock:
        async with self._main_lock:
            # Cleaner control flow: only construct the WaitCounterLock if
            # one isn't already cached.  The previous ``get(...) or
            # WaitCounterLock(...)`` form constructed a discarded lock on
            # every cache hit.  Mirrors pytgcalls upstream PR #319.
            lock = self._chat_lock.get(chat_id)
            if not lock:
                lock = WaitCounterLock(
                    self._remove_callback,
                    chat_id,
                )
                self._chat_lock[chat_id] = lock
            return lock
