"""Honeycomb Agent Coordination Protocol (HACP) v1.0.0 — Contracts & Envelopes."""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

HONEYCOMB_PROTOCOL_VERSION = "1.0.0"
DEFAULT_PROTOCOL_VERSION = HONEYCOMB_PROTOCOL_VERSION


def utc_now_iso() -> str:
    """Return current UTC timestamp formatted as ISO 8601."""
    return datetime.now(UTC).isoformat()


def compute_target_scope_hash(
    garden_target_id: str = "",
    node_id: str = "START",
    intent_or_prompt: str = "",
    extra_scope: str = "",
    *,
    target_id: str = "",
    intent_signature: str = "",
    mission_id: str = "",
    objective: str = "",
    turn_index: int | str | None = "",
    stage_id: str = "",
    **kwargs: Any,
) -> str:
    """Compute a deterministic SHA-256 scope hash for atomic idempotency locking."""
    effective_target = garden_target_id or target_id or str(kwargs.get("target") or "unknown-target")
    effective_node = (
        node_id
        if (node_id and node_id != "START")
        else (stage_id or objective or node_id or "START")
    )
    effective_intent = (
        intent_or_prompt
        or intent_signature
        or ":".join(
            str(part)
            for part in (mission_id, objective, turn_index)
            if part not in ("", None)
        )
    )
    normalized_target = effective_target.strip().lower()
    normalized_node = effective_node.strip().lower()
    normalized_intent = " ".join(effective_intent.strip().lower().split())
    normalized_extra = (extra_scope or "").strip().lower()
    canonical = f"{normalized_target}|{normalized_node}|{normalized_intent}|{normalized_extra}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def generate_resume_token(
    mission_id: str,
    session_id: str,
    turn_index: int = 0,
    target_scope_hash: str = "",
    state_snapshot: dict[str, Any] | None = None,
) -> str:
    """Generate a deterministic, integrity-signed resume token for paused missions."""
    issued_at = utc_now_iso()
    token_body: dict[str, Any] = {
        "v": HONEYCOMB_PROTOCOL_VERSION,
        "mission_id": mission_id,
        "session_id": session_id,
        "turn_index": int(turn_index),
        "target_scope_hash": target_scope_hash,
        "issued_at": issued_at,
        "nonce": uuid.uuid4().hex[:12],
    }
    if state_snapshot:
        token_body["state"] = state_snapshot
    raw_json = json.dumps(token_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    b64_body = base64.urlsafe_b64encode(raw_json).decode("ascii").rstrip("=")
    sig = hashlib.sha256(raw_json).hexdigest()[:16]
    return f"hcrt1.{b64_body}.{sig}"


def parse_resume_token(token: str) -> dict[str, Any]:
    """Decode and verify a Honeycomb resume token created by `generate_resume_token`."""
    if not token or not isinstance(token, str):
        raise ValueError("Resume token must be a non-empty string.")
    parts = token.strip().split(".")
    if len(parts) != 3 or parts[0] not in {"hcrt_v1", "hcrt1"}:
        raise ValueError(f"Invalid Honeycomb resume token format: {token!r}")
    b64_body, expected_sig = parts[1], parts[2]
    padding = "=" * (-len(b64_body) % 4)
    try:
        raw_json = base64.urlsafe_b64decode((b64_body + padding).encode("ascii"))
    except Exception as exc:
        raise ValueError(f"Malformed base64 payload in resume token: {exc}") from exc
    actual_sig = hashlib.sha256(raw_json).hexdigest()[:16]
    if actual_sig != expected_sig:
        raise ValueError("Resume token integrity signature mismatch.")
    decoded = json.loads(raw_json.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("Decoded resume token payload must be a JSON object.")
    return decoded


class HoneycombEdge(BaseModel):
    """Typed graph edge primitive carried inside a HoneycombPayload."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    source: str = Field(..., min_length=1, description="Source graph node identifier")
    target: str = Field(..., min_length=1, description="Target graph node identifier")
    relation: str = Field(..., min_length=1, description="Semantic or transition relation label")
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Edge transition confidence in [0.0, 1.0]",
    )


class HoneycombPayload(BaseModel):
    """Graph-primitive payload exchanged across the Honeycomb Protocol."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    node_id: str = Field(..., min_length=1, description="Current or discovered graph node ID")
    node_type: str = Field(..., min_length=1, description="Semantic node classification")
    edges: list[HoneycombEdge] = Field(
        default_factory=list,
        description="Directed graph edges observed or traversed",
    )
    confidence_score: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Aggregate epistemic confidence score in [0.0, 1.0]",
    )
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Open dictionary for raw target data and runtime telemetry",
    )


class HoneycombEnvelopeHeader(BaseModel):
    """Typed view of the required HoneycombEnvelope protocol header."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    protocol_version: str = Field(default=HONEYCOMB_PROTOCOL_VERSION, min_length=1)
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str = Field(..., min_length=1)
    source_agent_id: str = Field(..., min_length=1)
    target_scope_hash: str = Field(..., min_length=1)
    ttl_seconds: int = Field(default=300, gt=0)
    resume_token: str | None = Field(default=None)
    timestamp: str = Field(default_factory=utc_now_iso)


class HoneycombEnvelope(BaseModel):
    """Canonical Pydantic v2 message envelope for the Honeycomb Agent Coordination Protocol."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    protocol_version: str = Field(
        default=HONEYCOMB_PROTOCOL_VERSION,
        min_length=1,
        description="Semantic version of the Honeycomb Protocol (default '1.0.0')",
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="UUID v4 correlation identifier linking multi-hop swarm messages",
    )
    session_id: str = Field(
        ...,
        min_length=1,
        description="Isolated session identifier for the Scout or Queen wave",
    )
    source_agent_id: str = Field(
        ...,
        min_length=1,
        description="Identifier of the emitting agent (e.g. 'queen' or 'scout-001')",
    )
    target_scope_hash: str = Field(
        ...,
        min_length=1,
        description="Deterministic hash of the target scope used for atomic idempotency",
    )
    ttl_seconds: int = Field(
        default=300,
        gt=0,
        description="Time-to-live of the envelope and its claim lease in seconds",
    )
    resume_token: str | None = Field(
        default=None,
        description="Optional token allowing stateful resumption of paused missions",
    )
    timestamp: str = Field(
        default_factory=utc_now_iso,
        description="ISO 8601 UTC creation timestamp",
    )
    payload: HoneycombPayload = Field(
        ...,
        description="Graph-primitive payload carrying nodes, edges, confidence, and raw properties",
    )

    @model_validator(mode="before")
    @classmethod
    def _unpack_optional_header(cls, data: Any) -> Any:
        """Allow instantiation with either flat header fields or a nested `header` dict/model."""
        if isinstance(data, dict) and "header" in data:
            merged = dict(data)
            raw_header = merged.pop("header")
            if isinstance(raw_header, BaseModel):
                header_dict = raw_header.model_dump()
            elif isinstance(raw_header, dict):
                header_dict = dict(raw_header)
            else:
                raise TypeError("`header` must be a dict or HoneycombEnvelopeHeader instance.")
            for k, v in header_dict.items():
                merged.setdefault(k, v)
            return merged
        return data

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_uuid(cls, value: str) -> str:
        """Ensure correlation_id is a valid UUID (defaulting to UUID v4)."""
        cleaned = (value or "").strip()
        try:
            parsed = uuid.UUID(cleaned)
        except ValueError as exc:
            raise ValueError(f"correlation_id must be a valid UUID string, got {value!r}") from exc
        return str(parsed)

    @field_validator("timestamp")
    @classmethod
    def _validate_iso_timestamp(cls, value: str) -> str:
        """Ensure timestamp is a valid ISO 8601 UTC timestamp string."""
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("timestamp must not be empty.")
        normalized = cleaned.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"timestamp must be a valid ISO 8601 string, got {value!r}"
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()

    @property
    def header(self) -> HoneycombEnvelopeHeader:
        """Return the protocol header as a typed `HoneycombEnvelopeHeader` model."""
        return HoneycombEnvelopeHeader(
            protocol_version=self.protocol_version,
            correlation_id=self.correlation_id,
            session_id=self.session_id,
            source_agent_id=self.source_agent_id,
            target_scope_hash=self.target_scope_hash,
            ttl_seconds=self.ttl_seconds,
            resume_token=self.resume_token,
            timestamp=self.timestamp,
        )

    def is_expired(self, reference_time: datetime | None = None) -> bool:
        """Return True if the envelope has exceeded its `ttl_seconds` window."""
        now = reference_time or datetime.now(UTC)
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        created = datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        return now > (created + timedelta(seconds=self.ttl_seconds))

    def validate_semantic_graph(self) -> bool:
        """Verify semantic integrity of the graph payload (node, edges, and confidence bounds)."""
        if not self.payload.node_id.strip() or not self.payload.node_type.strip():
            return False
        if not (0.0 <= self.payload.confidence_score <= 1.0):
            return False
        for edge in self.payload.edges:
            if not (edge.source.strip() and edge.target.strip() and edge.relation.strip()):
                return False
            if not (0.0 <= edge.confidence <= 1.0):
                return False
        return True

    def derive_next(
        self,
        *,
        source_agent_id: str | None = None,
        target_scope_hash: str | None = None,
        correlation_id: str | None = None,
        node_id: str | None = None,
        node_type: str | None = None,
        edges: list[HoneycombEdge] | None = None,
        confidence_score: float | None = None,
        properties: dict[str, Any] | None = None,
        properties_update: dict[str, Any] | None = None,
        payload: HoneycombPayload | None = None,
        resume_token: str | None = None,
    ) -> HoneycombEnvelope:
        """Create a follow-up envelope preserving session_id and correlation_id."""
        if payload is not None:
            new_payload = payload
        else:
            merged_props = dict(self.payload.properties)
            if properties is not None:
                merged_props.update(properties)
            if properties_update is not None:
                merged_props.update(properties_update)
            new_payload = HoneycombPayload(
                node_id=node_id if node_id is not None else self.payload.node_id,
                node_type=node_type if node_type is not None else self.payload.node_type,
                edges=list(edges) if edges is not None else list(self.payload.edges),
                confidence_score=(
                    float(confidence_score)
                    if confidence_score is not None
                    else self.payload.confidence_score
                ),
                properties=merged_props,
            )
        return HoneycombEnvelope(
            protocol_version=self.protocol_version,
            correlation_id=correlation_id or self.correlation_id,
            session_id=self.session_id,
            source_agent_id=source_agent_id or self.source_agent_id,
            target_scope_hash=target_scope_hash or self.target_scope_hash,
            ttl_seconds=self.ttl_seconds,
            resume_token=resume_token if resume_token is not None else self.resume_token,
            timestamp=utc_now_iso(),
            payload=new_payload,
        )

    @staticmethod
    def compute_scope_hash(
        garden_target_id: str,
        node_id: str = "START",
        intent_or_prompt: str = "",
        extra_scope: str = "",
    ) -> str:
        """Convenience alias for `compute_target_scope_hash`."""
        return compute_target_scope_hash(
            garden_target_id=garden_target_id,
            node_id=node_id,
            intent_or_prompt=intent_or_prompt,
            extra_scope=extra_scope,
        )

    @staticmethod
    def create_resume_token(
        mission_id: str,
        session_id: str,
        turn_index: int = 0,
        target_scope_hash: str = "",
        state_snapshot: dict[str, Any] | None = None,
    ) -> str:
        """Convenience alias for `generate_resume_token`."""
        return generate_resume_token(
            mission_id=mission_id,
            session_id=session_id,
            turn_index=turn_index,
            target_scope_hash=target_scope_hash,
            state_snapshot=state_snapshot,
        )

    def __getitem__(self, item: str) -> Any:
        """Provide read-only mapping compatibility over envelope fields and payload.properties."""
        if item == "envelope":
            return self
        if item in self.__class__.model_fields:
            return getattr(self, item)
        if item in HoneycombPayload.model_fields:
            return getattr(self.payload, item)
        if item in self.payload.properties:
            return self.payload.properties[item]
        raise KeyError(item)

    def get(self, item: str, default: Any = None) -> Any:
        """Dict-compatible `.get()` lookup across envelope, payload, and properties."""
        try:
            return self[item]
        except KeyError:
            return default

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, str):
            return False
        return (
            item == "envelope"
            or item in self.__class__.model_fields
            or item in HoneycombPayload.model_fields
            or item in self.payload.properties
        )
