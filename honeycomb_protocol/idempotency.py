"""Atomic SQLite WAL idempotency lock and deduplication guard for the Honeycomb Protocol."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field

from honeycomb_protocol.protocol import HoneycombEnvelope, utc_now_iso

if TYPE_CHECKING:
    HoneycombLedger = Any


IDEMPOTENCY_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS honeycomb_idempotency_keys (
    target_scope_hash TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    source_agent_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    ttl_seconds INTEGER NOT NULL DEFAULT 300,
    resume_token TEXT DEFAULT NULL,
    envelope_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_honeycomb_idempotency_status
    ON honeycomb_idempotency_keys(status, expires_at);
"""


class IdempotencyStatus(str, Enum):
    """Lifecycle states of a target_scope_hash key in `honeycomb_idempotency_keys`."""

    CLAIMED = "CLAIMED"
    COMPLETED = "COMPLETED"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"


BLOCKING_IDEMPOTENCY_STATUSES: frozenset[str] = frozenset(
    {
        IdempotencyStatus.CLAIMED.value,
        IdempotencyStatus.COMPLETED.value,
    }
)


class IdempotencyRecord(BaseModel):
    """Persisted row in the SQLite `honeycomb_idempotency_keys` table."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    target_scope_hash: str = Field(..., min_length=1)
    status: IdempotencyStatus
    source_agent_id: str
    session_id: str
    correlation_id: str
    ttl_seconds: int = Field(default=300, gt=0)
    resume_token: str | None = None
    envelope_json: str = "{}"
    result_json: str = "{}"
    created_at: str
    updated_at: str
    expires_at: str


class IdempotencyClaimResult(BaseModel):
    """Result of an atomic `check_and_claim` operation against `honeycomb_idempotency_keys`."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    acquired: bool = Field(
        ...,
        description="True when the caller atomically claimed the key; False when blocked.",
    )
    discarded: bool = Field(
        ...,
        description="True when execution must be discarded immediately ('CLAIMED' or 'COMPLETED').",
    )
    target_scope_hash: str
    status: IdempotencyStatus
    existing_record: IdempotencyRecord | None = None
    reason: str = ""

    @property
    def owner_agent_id(self) -> str | None:
        """Return the `source_agent_id` that owns the existing claim, if any."""
        return (
            self.existing_record.source_agent_id
            if self.existing_record is not None
            else None
        )

    def __bool__(self) -> bool:
        """Allow truthiness evaluation (`if await guard.check_and_claim(...):`)."""
        return self.acquired


class HoneycombIdempotencyGuard:
    """Atomic SQLite WAL idempotency controller backed by `honeycomb_idempotency_keys`."""

    def __init__(
        self,
        db_path: str | Path | HoneycombLedger | None = None,
        *,
        ledger: HoneycombLedger | None = None,
        write_lock: asyncio.Lock | None = None,
    ) -> None:
        if db_path is not None and hasattr(db_path, "db_path") and not isinstance(db_path, (str, Path)):
            ledger = db_path  # type: ignore[assignment]
            db_path = None
        if ledger is not None:
            self.db_path = Path(ledger.db_path)
            self._write_lock = ledger._write_lock
        elif db_path is not None:
            self.db_path = Path(db_path)  # type: ignore[arg-type]
            self._write_lock = write_lock or asyncio.Lock()
        else:
            raise ValueError("Either `ledger` or `db_path` must be provided to HoneycombIdempotencyGuard.")
        self._ledger = ledger
        self._initialized = False

    async def _connect_writer(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.db_path, timeout=30.0)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA synchronous=NORMAL;")
        await conn.execute("PRAGMA foreign_keys=ON;")
        await conn.execute("PRAGMA busy_timeout=5000;")
        if not self._initialized:
            await conn.executescript(IDEMPOTENCY_TABLE_DDL)
            await conn.commit()
            self._initialized = True
        return conn

    async def initialize(self) -> None:
        """Ensure `honeycomb_idempotency_keys` exists in the SQLite WAL database."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                await conn.executescript(IDEMPOTENCY_TABLE_DDL)
                await conn.commit()
                self._initialized = True
            finally:
                await conn.close()

    @staticmethod
    def _row_to_record(row: aiosqlite.Row) -> IdempotencyRecord:
        return IdempotencyRecord(
            target_scope_hash=str(row["target_scope_hash"]),
            status=IdempotencyStatus(str(row["status"])),
            source_agent_id=str(row["source_agent_id"]),
            session_id=str(row["session_id"]),
            correlation_id=str(row["correlation_id"]),
            ttl_seconds=int(row["ttl_seconds"]),
            resume_token=row["resume_token"],
            envelope_json=str(row["envelope_json"] or "{}"),
            result_json=str(row["result_json"] or "{}"),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            expires_at=str(row["expires_at"]),
        )

    async def check_and_claim(
        self,
        target_scope_hash: str | HoneycombEnvelope,
        *,
        source_agent_id: str = "scout-agent",
        session_id: str = "default-session",
        correlation_id: str | None = None,
        ttl_seconds: int = 300,
        resume_token: str | None = None,
        envelope: HoneycombEnvelope | None = None,
        expire_stale: bool = True,
    ) -> IdempotencyClaimResult:
        """Atomically check and register `target_scope_hash` in `honeycomb_idempotency_keys`.

        If the key is already in status `'CLAIMED'` or `'COMPLETED'`, returns
        `IdempotencyClaimResult(acquired=False, discarded=True, ...)` so the caller
        can discard execution immediately before any agent processing or Gemini inference.
        """
        if isinstance(target_scope_hash, HoneycombEnvelope):
            envelope = target_scope_hash
            scope_hash = envelope.target_scope_hash
            source_agent_id = envelope.source_agent_id
            session_id = envelope.session_id
            correlation_id = envelope.correlation_id
            ttl_seconds = envelope.ttl_seconds
            resume_token = envelope.resume_token
        else:
            scope_hash = str(target_scope_hash).strip()
            if not scope_hash:
                raise ValueError("target_scope_hash must be a non-empty string.")
            if envelope is not None:
                source_agent_id = envelope.source_agent_id or source_agent_id
                session_id = envelope.session_id or session_id
                correlation_id = envelope.correlation_id or correlation_id
                ttl_seconds = envelope.ttl_seconds or ttl_seconds
                resume_token = envelope.resume_token if envelope.resume_token is not None else resume_token

        corr_id = correlation_id or str(uuid.uuid4())
        ttl = max(1, int(ttl_seconds))
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat()
        expires_iso = (now_dt + timedelta(seconds=ttl)).isoformat()
        env_json = (
            json.dumps(envelope.model_dump(mode="json"), ensure_ascii=False)
            if envelope is not None
            else "{}"
        )

        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                await conn.execute("BEGIN IMMEDIATE;")
                cursor = await conn.execute(
                    "SELECT * FROM honeycomb_idempotency_keys WHERE target_scope_hash = ?",
                    (scope_hash,),
                )
                existing_row = await cursor.fetchone()
                if existing_row is not None:
                    existing_rec = self._row_to_record(existing_row)
                    is_stale_claim = (
                        expire_stale
                        and existing_rec.status == IdempotencyStatus.CLAIMED
                        and existing_rec.expires_at < now_iso
                    )
                    if (
                        existing_rec.status.value in BLOCKING_IDEMPOTENCY_STATUSES
                        and not is_stale_claim
                    ):
                        await conn.rollback()
                        return IdempotencyClaimResult(
                            acquired=False,
                            discarded=True,
                            target_scope_hash=scope_hash,
                            status=existing_rec.status,
                            existing_record=existing_rec,
                            reason=(
                                f"Target scope hash '{scope_hash}' already in status "
                                f"'{existing_rec.status.value}' (claimed by '{existing_rec.source_agent_id}'); "
                                "discarding duplicate execution."
                            ),
                        )

                await conn.execute(
                    """
                    INSERT INTO honeycomb_idempotency_keys (
                        target_scope_hash, status, source_agent_id, session_id,
                        correlation_id, ttl_seconds, resume_token, envelope_json,
                        result_json, created_at, updated_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?)
                    ON CONFLICT(target_scope_hash) DO UPDATE SET
                        status = excluded.status,
                        source_agent_id = excluded.source_agent_id,
                        session_id = excluded.session_id,
                        correlation_id = excluded.correlation_id,
                        ttl_seconds = excluded.ttl_seconds,
                        resume_token = excluded.resume_token,
                        envelope_json = excluded.envelope_json,
                        updated_at = excluded.updated_at,
                        expires_at = excluded.expires_at
                    """,
                    (
                        scope_hash,
                        IdempotencyStatus.CLAIMED.value,
                        source_agent_id,
                        session_id,
                        corr_id,
                        ttl,
                        resume_token,
                        env_json,
                        now_iso,
                        now_iso,
                        expires_iso,
                    ),
                )
                await conn.commit()

                claimed_record = IdempotencyRecord(
                    target_scope_hash=scope_hash,
                    status=IdempotencyStatus.CLAIMED,
                    source_agent_id=source_agent_id,
                    session_id=session_id,
                    correlation_id=corr_id,
                    ttl_seconds=ttl,
                    resume_token=resume_token,
                    envelope_json=env_json,
                    result_json="{}",
                    created_at=now_iso,
                    updated_at=now_iso,
                    expires_at=expires_iso,
                )
                return IdempotencyClaimResult(
                    acquired=True,
                    discarded=False,
                    target_scope_hash=scope_hash,
                    status=IdempotencyStatus.CLAIMED,
                    existing_record=claimed_record,
                    reason="CLAIMED_ATOMICALLY",
                )
            except Exception:
                await conn.rollback()
                raise
            finally:
                await conn.close()

    async def mark_completed(
        self,
        target_scope_hash: str | HoneycombEnvelope,
        result: dict[str, Any] | HoneycombEnvelope | None = None,
        *,
        resume_token: str | None = None,
        result_digest: str | None = None,
        source_agent_id: str | None = None,
        result_node_id: str | None = None,
        confidence_score: float | None = None,
        **extra_result: Any,
    ) -> IdempotencyRecord | None:
        """Transition a claimed key to `'COMPLETED'` after successful inference and graph persistence."""
        if isinstance(target_scope_hash, HoneycombEnvelope):
            scope_hash = target_scope_hash.target_scope_hash
            if result is None:
                result = target_scope_hash
            if resume_token is None:
                resume_token = target_scope_hash.resume_token
        else:
            scope_hash = str(target_scope_hash).strip()

        if isinstance(result, HoneycombEnvelope):
            result_payload_json = json.dumps(result.model_dump(mode="json"), ensure_ascii=False)
        else:
            merged_result: dict[str, Any] = dict(result) if isinstance(result, dict) else {}
            if result_digest is not None:
                merged_result["result_digest"] = result_digest
            if source_agent_id is not None:
                merged_result["source_agent_id"] = source_agent_id
            if result_node_id is not None:
                merged_result["result_node_id"] = result_node_id
            if confidence_score is not None:
                merged_result["confidence_score"] = confidence_score
            if extra_result:
                merged_result.update(extra_result)
            result_payload_json = (
                json.dumps(merged_result, ensure_ascii=False, default=str)
                if merged_result
                else "{}"
            )

        now_iso = utc_now_iso()
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                if resume_token is not None:
                    await conn.execute(
                        """
                        UPDATE honeycomb_idempotency_keys
                        SET status = ?, result_json = ?, resume_token = ?, updated_at = ?
                        WHERE target_scope_hash = ?
                        """,
                        (
                            IdempotencyStatus.COMPLETED.value,
                            result_payload_json,
                            resume_token,
                            now_iso,
                            scope_hash,
                        ),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE honeycomb_idempotency_keys
                        SET status = ?, result_json = ?, updated_at = ?
                        WHERE target_scope_hash = ?
                        """,
                        (
                            IdempotencyStatus.COMPLETED.value,
                            result_payload_json,
                            now_iso,
                            scope_hash,
                        ),
                    )
                await conn.commit()
                cursor = await conn.execute(
                    "SELECT * FROM honeycomb_idempotency_keys WHERE target_scope_hash = ?",
                    (scope_hash,),
                )
                row = await cursor.fetchone()
                return self._row_to_record(row) if row is not None else None
            finally:
                await conn.close()

    async def release(self, target_scope_hash: str) -> bool:
        """Release a `'CLAIMED'` key (setting status to `'RELEASED'`) so it can be retried."""
        scope_hash = str(target_scope_hash).strip()
        now_iso = utc_now_iso()
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                cursor = await conn.execute(
                    """
                    UPDATE honeycomb_idempotency_keys
                    SET status = ?, updated_at = ?
                    WHERE target_scope_hash = ? AND status = ?
                    """,
                    (
                        IdempotencyStatus.RELEASED.value,
                        now_iso,
                        scope_hash,
                        IdempotencyStatus.CLAIMED.value,
                    ),
                )
                await conn.commit()
                return (cursor.rowcount or 0) > 0
            finally:
                await conn.close()

    async def get_record(self, target_scope_hash: str) -> IdempotencyRecord | None:
        """Return the current `IdempotencyRecord` for `target_scope_hash`, or None."""
        scope_hash = str(target_scope_hash).strip()
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                cursor = await conn.execute(
                    "SELECT * FROM honeycomb_idempotency_keys WHERE target_scope_hash = ?",
                    (scope_hash,),
                )
                row = await cursor.fetchone()
                return self._row_to_record(row) if row is not None else None
            finally:
                await conn.close()

    async def is_claimed_or_completed(self, target_scope_hash: str) -> bool:
        """Return True if `target_scope_hash` is currently `'CLAIMED'` or `'COMPLETED'`."""
        rec = await self.get_record(target_scope_hash)
        return rec is not None and rec.status.value in BLOCKING_IDEMPOTENCY_STATUSES

    async def list_records(self, status: str | None = None) -> list[IdempotencyRecord]:
        """List idempotency records, optionally filtered by status."""
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                if status:
                    cursor = await conn.execute(
                        "SELECT * FROM honeycomb_idempotency_keys WHERE status = ? ORDER BY updated_at DESC",
                        (status,),
                    )
                else:
                    cursor = await conn.execute(
                        "SELECT * FROM honeycomb_idempotency_keys ORDER BY updated_at DESC"
                    )
                rows = await cursor.fetchall()
                return [self._row_to_record(r) for r in rows]
            finally:
                await conn.close()

    async def clear_all(self) -> None:
        """Clear all keys from `honeycomb_idempotency_keys` (used during surgical reset)."""
        async with self._write_lock:
            conn = await self._connect_writer()
            try:
                await conn.execute("DELETE FROM honeycomb_idempotency_keys;")
                await conn.commit()
            finally:
                await conn.close()


HoneycombIdempotencyStore = HoneycombIdempotencyGuard
IdempotencyController = HoneycombIdempotencyGuard

