"""Standalone unit and lifecycle tests for the Honeycomb Agent Coordination Protocol (HACP v1.0.0)."""

from pathlib import Path
import uuid

import pytest
from pydantic import ValidationError

from honeycomb_protocol import (
    HoneycombEdge,
    HoneycombEnvelope,
    HoneycombIdempotencyGuard,
    HoneycombPayload,
    IdempotencyStatus,
    SwarmReflexBus,
    compute_target_scope_hash,
    generate_resume_token,
    parse_resume_token,
)


def test_honeycomb_envelope_and_payload_contract_validation() -> None:
    """Verify Pydantic v2 runtime validation for HoneycombEnvelope and HoneycombPayload."""
    scope_hash = compute_target_scope_hash(
        garden_target_id="+5511974627106",
        mission_id="mission-001",
        objective="SCOPE_DRIFT_DOW",
        turn_index=0,
    )
    payload = HoneycombPayload(
        node_id="entry_router",
        node_type="stage_probe",
        edges=[
            HoneycombEdge(
                source="entry_router",
                target="product_discovery",
                relation="DISCOVERS_STAGE",
                confidence=0.92,
            )
        ],
        confidence_score=0.88,
        properties={"raw_quote": "Notebook Gamer RTX 4060 por R$ 5.499 no Pix"},
    )
    envelope = HoneycombEnvelope(
        session_id="sess-001",
        source_agent_id="Scout-001",
        target_scope_hash=scope_hash,
        ttl_seconds=45,
        payload=payload,
    )

    assert envelope.protocol_version == "1.0.0"
    assert uuid.UUID(envelope.correlation_id).version == 4
    assert envelope.session_id == "sess-001"
    assert envelope.source_agent_id == "Scout-001"
    assert envelope.target_scope_hash == scope_hash
    assert envelope.ttl_seconds == 45
    assert envelope.resume_token is None
    assert envelope.payload.node_id == "entry_router"
    assert envelope.payload.confidence_score == 0.88
    assert len(envelope.payload.edges) == 1
    assert envelope.validate_semantic_graph() is True

    with pytest.raises(ValidationError):
        HoneycombPayload(
            node_id="n1",
            node_type="probe",
            edges=[],
            confidence_score=1.5,
            properties={},
        )


@pytest.mark.asyncio
async def test_honeycomb_idempotency_sqlite_wal_atomic_lock(tmp_path: Path) -> None:
    """Verify atomic pre-inference lock on SQLite WAL prevents duplicate agent work."""
    db_path = tmp_path / "honeycomb_test.sqlite3"
    guard = HoneycombIdempotencyGuard(db_path)
    await guard.initialize()

    scope_hash = compute_target_scope_hash(
        garden_target_id="target-bot",
        mission_id="mission-idem-1",
        objective="SCOPE_DRIFT_DOW",
        turn_index=0,
    )

    claim_1 = await guard.check_and_claim(
        scope_hash,
        source_agent_id="Scout-01",
        session_id="sess-idem-1",
        ttl_seconds=60,
    )
    assert claim_1.acquired is True
    assert claim_1.discarded is False
    assert claim_1.status == IdempotencyStatus.CLAIMED

    claim_2 = await guard.check_and_claim(
        scope_hash,
        source_agent_id="Scout-02",
        session_id="sess-idem-2",
        ttl_seconds=60,
    )
    assert claim_2.acquired is False
    assert claim_2.discarded is True
    assert claim_2.status == IdempotencyStatus.CLAIMED

    await guard.mark_completed(scope_hash, result={"verified": True})
    claim_3 = await guard.check_and_claim(
        scope_hash,
        source_agent_id="Scout-03",
        session_id="sess-idem-3",
    )
    assert claim_3.acquired is False
    assert claim_3.discarded is True
    assert claim_3.status == IdempotencyStatus.COMPLETED


@pytest.mark.asyncio
async def test_dynamic_handshake_double_ack_and_teardown() -> None:
    """Verify Pillar I handshake, Pillar III double-ack, and Pillar IV session queue teardown."""
    bus = SwarmReflexBus()
    scope_hash = compute_target_scope_hash(
        garden_target_id="enterprise-surface",
        node_id="identification",
        intent_or_prompt="GUEST_POLICY_RULES",
    )
    envelope = HoneycombEnvelope(
        session_id="sess-dynamic-01",
        source_agent_id="Scout-Bia",
        target_scope_hash=scope_hash,
        payload=HoneycombPayload(
            node_id="identification",
            node_type="auth_boundary_probe",
            confidence_score=0.92,
            properties={"capabilities": ["graph_probe", "stigmergic_trace", "guest_bypass"]},
        ),
    )

    q1 = bus.create_session_queue("sess-dynamic-01")
    q2 = bus.create_session_queue("sess-dynamic-01")

    init_ev = await bus.initiate_session(envelope)
    assert init_ev["payload"]["advertised_capabilities"] == [
        "graph_probe",
        "stigmergic_trace",
        "guest_bypass",
    ]

    ack_ev = await bus.acknowledge_handshake(
        envelope,
        responder_agent_id="Queen",
        authorized_scopes=[scope_hash, "scope:discovery:read"],
    )
    assert str(ack_ev["payload"]["scoped_grant_token"]).startswith("hcgrant1.")

    dack_ev = await bus.verify_double_ack(envelope, physical_transport_ok=True)
    assert dack_ev is not None

    teardown_ev = await bus.teardown_session(envelope, reason="COMPLETED")
    assert teardown_ev["payload"]["deallocated_queues"] == 2
    assert q1.empty()
    assert q2.empty()


def test_resume_token_signing_and_verification() -> None:
    """Verify cryptographic hcrt1 resume token generation and parsing."""
    scope_hash = compute_target_scope_hash(
        garden_target_id="surface-01",
        mission_id="m-100",
        objective="TOKEN_DRAIN",
        turn_index=4,
    )
    token = generate_resume_token(
        mission_id="m-100",
        session_id="s-100",
        turn_index=4,
        target_scope_hash=scope_hash,
    )
    assert token.startswith("hcrt1.")
    parsed = parse_resume_token(token)
    assert parsed["mission_id"] == "m-100"
    assert parsed["session_id"] == "s-100"
    assert parsed["turn_index"] == 4
    assert parsed["target_scope_hash"] == scope_hash
