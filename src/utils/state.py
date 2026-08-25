"""Watermark and run-state manager. Persists to JSON files in state/.

H-7: All reads and writes to C{watermarks.json} are protected by a
L{filelock.FileLock}.  Without the lock, two concurrent pipeline processes
(e.g. a scheduled run and a manual backfill) can interleave their
read-modify-write cycles and silently clobber each other's watermark,
causing rows to be skipped or re-processed on the next run.

The lock file (C{watermarks.json.lock}) is created alongside the state
file and is safe to delete manually if a process crashes mid-write.
"""

from __future__ import annotations

import json
from pathlib import Path

from filelock import FileLock


class StateManager:
    """
    Manage persistent watermark state and run history for the pipeline.

    All mutations to C{watermarks.json} are serialised through a
    L{filelock.FileLock} to prevent concurrent-run corruption (H-7).

    @ivar _dir:            Root directory for all state files.
    @ivar _watermark_file: Path to C{watermarks.json}.
    @ivar _watermark_lock: L{FileLock} guarding C{watermarks.json}.
    @ivar _runs_file:      Path to the append-only C{run_history.jsonl}.
    """

    def __init__(self, state_dir: Path) -> None:
        self._dir = state_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._watermark_file = self._dir / "watermarks.json"
        self._watermark_lock = FileLock(str(self._watermark_file) + ".lock")
        self._runs_file = self._dir / "run_history.jsonl"

    # ── watermarks ──────────────────────────────────────────────────────────

    def get_watermark(self, source: str) -> str | None:
        """
        Return the stored watermark for C{source}, or C{None} if absent.

        Acquires the file lock before reading so the value is consistent
        even when a concurrent process is mid-write.

        @param source: Watermark key (e.g. C{"products_updated_at"}).
        @return: Stored watermark string, or C{None}.
        """
        with self._watermark_lock:
            if not self._watermark_file.exists():
                return None
            data: dict[str, str] = json.loads(self._watermark_file.read_text())
            return data.get(source)

    def set_watermark(self, source: str, value: str) -> None:
        """
        Persist C{value} as the watermark for C{source} (H-7).

        The entire read-modify-write cycle is executed under a
        L{filelock.FileLock}, preventing concurrent processes from
        clobbering each other's updates.

        @param source: Watermark key (e.g. C{"products_updated_at"}).
        @param value:  New watermark value to store.
        """
        with self._watermark_lock:
            data: dict[str, str] = {}
            if self._watermark_file.exists():
                data = json.loads(self._watermark_file.read_text())
            data[source] = value
            self._watermark_file.write_text(json.dumps(data, indent=2))

    # ── run history ─────────────────────────────────────────────────────────

    def record_run(self, metadata: dict) -> None:
        """
        Append C{metadata} as a JSON line to C{run_history.jsonl}.

        File-append is used directly; POSIX guarantees that short appends
        to a single file are atomic at the OS level, so no lock is needed
        for this write-only operation.

        @param metadata: Run metadata dict to serialise.
        """
        with self._runs_file.open("a") as f:
            f.write(json.dumps(metadata) + "\n")
