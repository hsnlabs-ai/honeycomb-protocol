"""In-process tactical reflex event bus (<10ms latency) with HoneycombHandshake lifecycle protocol."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from honeycomb_protocol.protocol import HoneycombEnvelope, utc_now_iso

ReflexSubscriber = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]

HONEYCOMB_SESSION_INIT = "HONEYCOMB_SESSION_INIT"
HONEYCOMB_HANDSHAKE_ACK = "HONEYCOMB_HANDSHAKE_ACK"
HONEYCOMB_DOUBLE_ACK = "HONEYCOMB_DOUBLE_ACK"
HONEYCOMB_SESSION_TEARDOWN = "HONEYCOMB_SESSION_TEARDOWN"

LIFECYCLE_TOPICS: frozenset[str] = frozenset(
    {
        HONEYCOMB_SESSION_INIT,
        HONEYCOMB_HANDSHAKE_ACK,
        HONEYCOMB_DOUBLE_ACK,
        HONEYCOMB_SESSION_TEARDOWN,
    }
)


class ReflexEvent(dict[str, Any]):
    """Dictionary event emitted on `SwarmReflexBus` with attribute convenience accessors."""

    @property
    def topic(self) -> str:
        return str(self.get("type", ""))

    @property
    def type(self) -> str:
        return str(self.get("type", ""))

    @property
    def payload(self) -> dict[str, Any]:
        val = self.get("payload")
        return val if isinstance(val, dict) else {}

    @property
    def timestamp(self) -> str:
        return str(self.get("timestamp", ""))


class DoubleAckEvent(ReflexEvent):
    """`ReflexEvent` for `HONEYCOMB_DOUBLE_ACK` supporting both dict access and `(event, verified)` unpacking."""

    def __iter__(self):
        yield self
        yield bool(self.payload.get("double_ack_verified", False))


class HoneycombLifecycleEvent(str, Enum):
    """Formal lifecycle event types standardized by the Honeycomb Protocol."""

    HONEYCOMB_SESSION_INIT = HONEYCOMB_SESSION_INIT
    HONEYCOMB_HANDSHAKE_ACK = HONEYCOMB_HANDSHAKE_ACK
    HONEYCOMB_DOUBLE_ACK = HONEYCOMB_DOUBLE_ACK
    HONEYCOMB_SESSION_TEARDOWN = HONEYCOMB_SESSION_TEARDOWN


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class HoneycombHandshakeRecord(BaseModel):
    """Stateful tracking record for a HoneycombHandshake session on the tactical reflex bus."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    session_id: str
    correlation_id: str
    source_agent_id: str
    responder_agent_id: str = "queen"
    target_scope_hash: str
    stage: str = "INITIATED"
    advertised_capabilities: list[str] = Field(
        default_factory=lambda: ["graph_probe", "stigmergic_trace"]
    )
    authorized_scopes: list[str] = Field(default_factory=list)
    scoped_grant_token: str | None = None
    physical_transport_ack: bool = False
    semantic_graph_ack: bool = False
    double_ack_verified: bool = False
    resume_token: str | None = None
    teardown_reason: str = ""
    deallocated_queues: int = 0
    events_emitted: list[str] = Field(default_factory=list)
    started_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)

    @property
    def torn_down(self) -> bool:
        """Return True if this session has completed HONEYCOMB_SESSION_TEARDOWN."""
        return (
            self.stage == HONEYCOMB_SESSION_TEARDOWN
            or HONEYCOMB_SESSION_TEARDOWN in self.events_emitted
        )


def _compute_scoped_grant_token(
    session_id: str,
    source_agent_id: str,
    responder_agent_id: str,
    authorized_scopes: list[str],
    advertised_capabilities: list[str],
) -> str:
    """Compute a deterministic capability grant token for a dynamic handshake."""
    canonical = (
        f"{session_id}|{source_agent_id}|{responder_agent_id}|"
        f"{','.join(sorted(authorized_scopes))}|{','.join(sorted(advertised_capabilities))}"
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"hcgrant1.{digest}"


class HoneycombHandshake:
    """Coordinator for the 4-stage Honeycomb Protocol session lifecycle on SwarmReflexBus.

    Standardizes:
    1. `HONEYCOMB_SESSION_INIT`
    2. `HONEYCOMB_HANDSHAKE_ACK`
    3. `HONEYCOMB_DOUBLE_ACK` (physical transport ACK + semantic graph validation)
    4. `HONEYCOMB_SESSION_TEARDOWN`
    """

    def __init__(
        self,
        bus: SwarmReflexBus | None = None,
        *,
        session_id: str | None = None,
        source_agent_id: str | None = None,
        target_scope_hash: str | None = None,
    ) -> None:
        self._bus = bus
        self.default_session_id = session_id
        self.default_source_agent_id = source_agent_id
        self.default_target_scope_hash = target_scope_hash
        self._sessions: dict[str, HoneycombHandshakeRecord] = {}

    def bind_bus(self, bus: SwarmReflexBus) -> None:
        """Bind or rebind this handshake coordinator to a `SwarmReflexBus` instance."""
        self._bus = bus

    def get_session(self, session_id: str) -> HoneycombHandshakeRecord | None:
        """Return the tracked `HoneycombHandshakeRecord` for `session_id`, if any."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[HoneycombHandshakeRecord]:
        """Return all tracked handshake session records."""
        return list(self._sessions.values())

    @staticmethod
    def validate_semantic_graph_payload(envelope: HoneycombEnvelope) -> bool:
        """Validate the semantic graph primitives inside a `HoneycombEnvelope`."""
        if not isinstance(envelope, HoneycombEnvelope):
            envelope = HoneycombEnvelope.model_validate(envelope)
        return envelope.validate_semantic_graph()

    async def initiate_session(
        self,
        envelope: HoneycombEnvelope,
        *,
        capabilities: list[str] | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Emit `HONEYCOMB_SESSION_INIT` to open a coordinated Honeycomb session."""
        if not isinstance(envelope, HoneycombEnvelope):
            envelope = HoneycombEnvelope.model_validate(envelope)
        now_iso = _utc_now_iso()
        raw_caps = capabilities
        if raw_caps is None:
            prop_caps = envelope.payload.properties.get("capabilities")
            if isinstance(prop_caps, list) and prop_caps:
                raw_caps = [str(c) for c in prop_caps]
            else:
                raw_caps = ["graph_probe", "stigmergic_trace"]
        record = HoneycombHandshakeRecord(
            session_id=envelope.session_id,
            correlation_id=envelope.correlation_id,
            source_agent_id=envelope.source_agent_id,
            target_scope_hash=envelope.target_scope_hash,
            stage=HONEYCOMB_SESSION_INIT,
            advertised_capabilities=list(raw_caps),
            resume_token=envelope.resume_token,
            events_emitted=[HONEYCOMB_SESSION_INIT],
            started_at=now_iso,
            updated_at=now_iso,
        )
        self._sessions[envelope.session_id] = record
        payload = {
            "lifecycle_event": HONEYCOMB_SESSION_INIT,
            "session_id": envelope.session_id,
            "correlation_id": envelope.correlation_id,
            "source_agent_id": envelope.source_agent_id,
            "target_scope_hash": envelope.target_scope_hash,
            "advertised_capabilities": record.advertised_capabilities,
            "resume_token": envelope.resume_token,
            "envelope": envelope.model_dump(mode="json"),
            "handshake": record.model_dump(mode="json"),
        }
        if self._bus is not None:
            return await self._bus.publish(
                HONEYCOMB_SESSION_INIT,
                payload,
                background_lifecycle=background,
            )
        return {"type": HONEYCOMB_SESSION_INIT, "timestamp": now_iso, "payload": payload}

    async def acknowledge_handshake(
        self,
        envelope: HoneycombEnvelope,
        *,
        responder_agent_id: str = "queen",
        authorized_scopes: list[str] | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Emit `HONEYCOMB_HANDSHAKE_ACK` confirming session registration and scope lock."""
        if not isinstance(envelope, HoneycombEnvelope):
            envelope = HoneycombEnvelope.model_validate(envelope)
        now_iso = _utc_now_iso()
        scopes = (
            [str(s) for s in authorized_scopes]
            if authorized_scopes
            else [envelope.target_scope_hash]
        )
        record = self._sessions.get(envelope.session_id)
        if record is None:
            grant_token = _compute_scoped_grant_token(
                session_id=envelope.session_id,
                source_agent_id=envelope.source_agent_id,
                responder_agent_id=responder_agent_id,
                authorized_scopes=scopes,
                advertised_capabilities=["graph_probe", "stigmergic_trace"],
            )
            record = HoneycombHandshakeRecord(
                session_id=envelope.session_id,
                correlation_id=envelope.correlation_id,
                source_agent_id=envelope.source_agent_id,
                responder_agent_id=responder_agent_id,
                target_scope_hash=envelope.target_scope_hash,
                stage=HONEYCOMB_HANDSHAKE_ACK,
                authorized_scopes=scopes,
                scoped_grant_token=grant_token,
                resume_token=envelope.resume_token,
                events_emitted=[HONEYCOMB_HANDSHAKE_ACK],
                started_at=now_iso,
                updated_at=now_iso,
            )
            self._sessions[envelope.session_id] = record
        else:
            record.responder_agent_id = responder_agent_id
            record.authorized_scopes = scopes
            record.scoped_grant_token = _compute_scoped_grant_token(
                session_id=envelope.session_id,
                source_agent_id=envelope.source_agent_id,
                responder_agent_id=responder_agent_id,
                authorized_scopes=scopes,
                advertised_capabilities=record.advertised_capabilities,
            )
            record.stage = HONEYCOMB_HANDSHAKE_ACK
            record.updated_at = now_iso
            record.events_emitted = [*record.events_emitted, HONEYCOMB_HANDSHAKE_ACK]

        payload = {
            "lifecycle_event": HONEYCOMB_HANDSHAKE_ACK,
            "session_id": envelope.session_id,
            "correlation_id": envelope.correlation_id,
            "source_agent_id": envelope.source_agent_id,
            "responder_agent_id": responder_agent_id,
            "target_scope_hash": envelope.target_scope_hash,
            "advertised_capabilities": record.advertised_capabilities,
            "authorized_scopes": record.authorized_scopes,
            "scoped_grant_token": record.scoped_grant_token,
            "envelope": envelope.model_dump(mode="json"),
            "handshake": record.model_dump(mode="json"),
        }
        if self._bus is not None:
            return await self._bus.publish(
                HONEYCOMB_HANDSHAKE_ACK,
                payload,
                background_lifecycle=background,
            )
        return {"type": HONEYCOMB_HANDSHAKE_ACK, "timestamp": now_iso, "payload": payload}

    async def verify_double_ack(
        self,
        envelope: HoneycombEnvelope,
        *,
        physical_transport_ok: bool = True,
        transport_confirmed: bool | None = None,
        transport_delivered: bool | None = None,
        semantic_graph_ok: bool | None = None,
        validator_agent_id: str | None = None,
        background: bool = False,
    ) -> ReflexEvent:
        """Emit `HONEYCOMB_DOUBLE_ACK` combining physical transport ACK + semantic graph validation."""
        if not isinstance(envelope, HoneycombEnvelope):
            envelope = HoneycombEnvelope.model_validate(envelope)
        now_iso = _utc_now_iso()
        if transport_confirmed is not None:
            transport_ack = bool(transport_confirmed)
        elif transport_delivered is not None:
            transport_ack = bool(transport_delivered)
        else:
            transport_ack = bool(physical_transport_ok)
        semantic_ack = (
            self.validate_semantic_graph_payload(envelope)
            if semantic_graph_ok is None
            else bool(semantic_graph_ok)
        )
        double_ack_ok = transport_ack and semantic_ack

        record = self._sessions.get(envelope.session_id)
        if record is None:
            record = HoneycombHandshakeRecord(
                session_id=envelope.session_id,
                correlation_id=envelope.correlation_id,
                source_agent_id=envelope.source_agent_id,
                responder_agent_id=validator_agent_id or "queen",
                target_scope_hash=envelope.target_scope_hash,
                stage=HONEYCOMB_DOUBLE_ACK if double_ack_ok else "DOUBLE_ACK_REJECTED",
                physical_transport_ack=transport_ack,
                semantic_graph_ack=semantic_ack,
                double_ack_verified=double_ack_ok,
                resume_token=envelope.resume_token,
                events_emitted=[HONEYCOMB_DOUBLE_ACK],
                started_at=now_iso,
                updated_at=now_iso,
            )
            self._sessions[envelope.session_id] = record
        else:
            if validator_agent_id:
                record.responder_agent_id = validator_agent_id
            record.physical_transport_ack = transport_ack
            record.semantic_graph_ack = semantic_ack
            record.double_ack_verified = double_ack_ok
            record.stage = HONEYCOMB_DOUBLE_ACK if double_ack_ok else "DOUBLE_ACK_REJECTED"
            record.updated_at = now_iso
            record.events_emitted = [*record.events_emitted, HONEYCOMB_DOUBLE_ACK]

        payload = {
            "lifecycle_event": HONEYCOMB_DOUBLE_ACK,
            "session_id": envelope.session_id,
            "correlation_id": envelope.correlation_id,
            "source_agent_id": envelope.source_agent_id,
            "validator_agent_id": validator_agent_id or record.responder_agent_id,
            "target_scope_hash": envelope.target_scope_hash,
            "transport_ack": transport_ack,
            "physical_transport_ack": transport_ack,
            "semantic_graph_ack": semantic_ack,
            "double_ack_verified": double_ack_ok,
            "node_id": envelope.payload.node_id,
            "node_type": envelope.payload.node_type,
            "edges_count": len(envelope.payload.edges),
            "confidence_score": envelope.payload.confidence_score,
            "envelope": envelope.model_dump(mode="json"),
            "handshake": record.model_dump(mode="json"),
        }
        if self._bus is not None:
            published = await self._bus.publish(
                HONEYCOMB_DOUBLE_ACK,
                payload,
                background_lifecycle=background,
            )
            return DoubleAckEvent(published)
        return DoubleAckEvent({"type": HONEYCOMB_DOUBLE_ACK, "timestamp": now_iso, "payload": payload})

    async def teardown_session(
        self,
        envelope: HoneycombEnvelope,
        *,
        reason: str = "COMPLETED",
        resume_token: str | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Emit `HONEYCOMB_SESSION_TEARDOWN` to cleanly close or pause a Honeycomb session."""
        if not isinstance(envelope, HoneycombEnvelope):
            envelope = HoneycombEnvelope.model_validate(envelope)
        now_iso = _utc_now_iso()
        effective_token = resume_token if resume_token is not None else envelope.resume_token
        deallocated_count = (
            self._bus.destroy_session_queues(envelope.session_id)
            if self._bus is not None
            else 0
        )

        record = self._sessions.get(envelope.session_id)
        if record is None:
            record = HoneycombHandshakeRecord(
                session_id=envelope.session_id,
                correlation_id=envelope.correlation_id,
                source_agent_id=envelope.source_agent_id,
                target_scope_hash=envelope.target_scope_hash,
                stage=HONEYCOMB_SESSION_TEARDOWN,
                resume_token=effective_token,
                teardown_reason=reason,
                deallocated_queues=deallocated_count,
                events_emitted=[HONEYCOMB_SESSION_TEARDOWN],
                started_at=now_iso,
                updated_at=now_iso,
            )
            self._sessions[envelope.session_id] = record
        else:
            record.stage = HONEYCOMB_SESSION_TEARDOWN
            record.resume_token = effective_token
            record.teardown_reason = reason
            record.deallocated_queues = deallocated_count
            record.updated_at = now_iso
            record.events_emitted = [*record.events_emitted, HONEYCOMB_SESSION_TEARDOWN]

        payload = {
            "lifecycle_event": HONEYCOMB_SESSION_TEARDOWN,
            "session_id": envelope.session_id,
            "correlation_id": envelope.correlation_id,
            "source_agent_id": envelope.source_agent_id,
            "target_scope_hash": envelope.target_scope_hash,
            "reason": reason,
            "resume_token": effective_token,
            "deallocated_queues": deallocated_count,
            "envelope": envelope.model_dump(mode="json"),
            "handshake": record.model_dump(mode="json"),
        }
        if self._bus is not None:
            return await self._bus.publish(
                HONEYCOMB_SESSION_TEARDOWN,
                payload,
                background_lifecycle=background,
            )
        return {"type": HONEYCOMB_SESSION_TEARDOWN, "timestamp": now_iso, "payload": payload}


class SwarmReflexBus:
    """Zero-broker asyncio tactical reflex bus (<10ms) with HoneycombHandshake support."""

    HAZARD_TOPICS = frozenset(
        {
            "GUARDRAIL_HAZARD_DETECTED",
            "ROUTE_SATURATED",
            "SCOUT_HEAT_HIGH",
            "TARGET_THROTTLE_SIGNAL",
        }
    )
    TASK_TOPICS = frozenset(
        {
            "TASK_DELEGATED",
            "TASK_CLAIMED",
            "TASK_COMPLETED",
        }
    )
    LIFECYCLE_TOPICS = LIFECYCLE_TOPICS
    SUPPORTED_TOPICS = HAZARD_TOPICS | TASK_TOPICS | LIFECYCLE_TOPICS

    def __init__(self, max_history: int = 200) -> None:
        self._callbacks: list[ReflexSubscriber] = []
        self._queues: list[tuple[asyncio.Queue[dict[str, Any]], bool, bool]] = []
        self._session_queues: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}
        self._max_history = max_history
        self.recent_events: list[dict[str, Any]] = []
        self.handshake = HoneycombHandshake(bus=self)

    def subscribe(self, callback: ReflexSubscriber) -> None:
        """Register an async callback subscriber."""
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def unsubscribe(self, callback: ReflexSubscriber) -> None:
        """Remove an async callback subscriber."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def create_queue(
        self,
        maxsize: int = 128,
        include_task_events: bool = False,
        include_lifecycle_events: bool = False,
        *,
        session_id: str | None = None,
    ) -> asyncio.Queue[dict[str, Any]]:
        """Create and register an asyncio.Queue for pull-based tactical consumption."""
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self._queues.append((q, include_task_events, include_lifecycle_events))
        if session_id:
            self._session_queues.setdefault(session_id, []).append(q)
        return q

    def create_session_queue(
        self,
        session_id: str,
        maxsize: int = 128,
        include_task_events: bool = True,
        include_lifecycle_events: bool = True,
    ) -> asyncio.Queue[dict[str, Any]]:
        """Create a temporary queue bound to `session_id` that is destroyed on teardown."""
        return self.create_queue(
            maxsize=maxsize,
            include_task_events=include_task_events,
            include_lifecycle_events=include_lifecycle_events,
            session_id=session_id,
        )

    def destroy_session_queues(self, session_id: str) -> int:
        """Drain and remove all temporary queues bound to `session_id`."""
        bound = self._session_queues.pop(session_id, [])
        if not bound:
            return 0
        bound_set = {id(q) for q in bound}
        self._queues = [
            entry for entry in self._queues if id(entry[0]) not in bound_set
        ]
        for q in bound:
            while not q.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
        return len(bound)

    async def publish(
        self,
        event_type: str,
        payload: dict[str, Any] | HoneycombEnvelope,
        *,
        background_lifecycle: bool = False,
    ) -> dict[str, Any]:
        """Broadcast a tactical reflex or Honeycomb lifecycle event in <10ms."""
        t0 = time.perf_counter()
        envelope_obj: HoneycombEnvelope | None = None
        if isinstance(payload, HoneycombEnvelope):
            envelope_obj = payload
            serialized_payload: dict[str, Any] = {
                "protocol_version": payload.protocol_version,
                "correlation_id": payload.correlation_id,
                "session_id": payload.session_id,
                "source_agent_id": payload.source_agent_id,
                "target_scope_hash": payload.target_scope_hash,
                "ttl_seconds": payload.ttl_seconds,
                "resume_token": payload.resume_token,
                "timestamp": payload.timestamp,
                "node_id": payload.payload.node_id,
                "node_type": payload.payload.node_type,
                "confidence_score": payload.payload.confidence_score,
                "edges": [e.model_dump(mode="json") for e in payload.payload.edges],
                "properties": dict(payload.payload.properties),
                "envelope": payload.model_dump(mode="json"),
            }
        else:
            serialized_payload = dict(payload)

        event = ReflexEvent(
            {
                "type": event_type,
                "timestamp": _utc_now_iso(),
                "monotonic_ns": time.perf_counter_ns(),
                "payload": serialized_payload,
            }
        )
        if envelope_obj is not None:
            event["envelope"] = envelope_obj

        self.recent_events.append(event)
        if len(self.recent_events) > self._max_history:
            self.recent_events = self.recent_events[-self._max_history :]

        is_task_topic = event_type in self.TASK_TOPICS
        is_lifecycle_topic = event_type in self.LIFECYCLE_TOPICS
        for q, include_tasks, include_lifecycle in list(self._queues):
            if is_task_topic and not include_tasks:
                continue
            if is_lifecycle_topic and background_lifecycle and not (include_tasks or include_lifecycle):
                continue
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(event)

        for cb in list(self._callbacks):
            with contextlib.suppress(RuntimeError, OSError, ValueError):
                await cb(event)

        event["dispatch_latency_ms"] = round((time.perf_counter() - t0) * 1000.0, 4)
        return event

    async def initiate_session(
        self,
        envelope: HoneycombEnvelope,
        *,
        capabilities: list[str] | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Delegate `HONEYCOMB_SESSION_INIT` to `self.handshake`."""
        return await self.handshake.initiate_session(
            envelope,
            capabilities=capabilities,
            background=background,
        )

    async def acknowledge_handshake(
        self,
        envelope: HoneycombEnvelope,
        *,
        responder_agent_id: str = "queen",
        authorized_scopes: list[str] | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Delegate `HONEYCOMB_HANDSHAKE_ACK` to `self.handshake`."""
        return await self.handshake.acknowledge_handshake(
            envelope,
            responder_agent_id=responder_agent_id,
            authorized_scopes=authorized_scopes,
            background=background,
        )

    async def verify_double_ack(
        self,
        envelope: HoneycombEnvelope,
        *,
        physical_transport_ok: bool = True,
        semantic_graph_ok: bool | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Delegate `HONEYCOMB_DOUBLE_ACK` to `self.handshake`."""
        return await self.handshake.verify_double_ack(
            envelope,
            physical_transport_ok=physical_transport_ok,
            semantic_graph_ok=semantic_graph_ok,
            background=background,
        )

    async def teardown_session(
        self,
        envelope: HoneycombEnvelope,
        *,
        reason: str = "COMPLETED",
        resume_token: str | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Delegate `HONEYCOMB_SESSION_TEARDOWN` to `self.handshake`."""
        return await self.handshake.teardown_session(
            envelope,
            reason=reason,
            resume_token=resume_token,
            background=background,
        )
