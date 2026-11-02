# Honeycomb Protocol (`HACP v1.0.0`)

**Open Stigmergic Coordination Protocol for Long-Horizon Autonomous Agent Swarms**  
Maintained by **hsnlabs** (`https://hsnlabs.ai`) · Released under the **MIT License**

- **Official Website (`hsnlabs.ai`):** [`https://hsnlabs.ai/en/products/honeycomb-protocol/`](https://hsnlabs.ai/en/products/honeycomb-protocol/) ([PT-BR](https://hsnlabs.ai/produtos/honeycomb-protocol/))
- **Public GitHub Repository:** [`https://github.com/hsnlabs-ai/honeycomb-protocol`](https://github.com/hsnlabs-ai/honeycomb-protocol)
- **Formal RFC Specification (`HACP-RFC-0001`):** [`docs/honeycomb/SPECIFICATION.md`](docs/SPECIFICATION.md)
- **Technical Launch Whitepaper (EN):** [`docs/honeycomb/WHITEPAPER.md`](docs/WHITEPAPER.md) ([Read on `hsnlabs.ai`](https://hsnlabs.ai/en/products/honeycomb-protocol/whitepaper/))
- **Automated Test Suite:** [`tests/test_honeycomb_protocol.py`](tests/test_honeycomb_protocol.py)

---

## Why Honeycomb?

Honeycomb operates at the **semantic decentralized coordination layer**—complementing vertical tool-binding protocols like MCP and local workflow graphs like LangGraph—to eliminate the four core failure modes of multi-agent swarms:

1. **Zero Destructive Concurrency:** Pre-inference atomic `SHA-256` scope locks (`BEGIN IMMEDIATE` on SQLite WAL) prevent multiple agents from colliding on the same target branch.
2. **78%–91% Token Overhead Reduction:** Replaces quadratic $O(N^2)$ natural language group chat with compact, causally-linked graph primitives (`HoneycombEnvelope`).
3. **Multi-Day Resilience (`hcrt1` Resume Tokens):** Cryptographically signed continuation tokens allow missions to pause and resume across quota resets or network drops without operational amnesia.
4. **Zero Zombie Channels:** Mandatory `HONEYCOMB_SESSION_TEARDOWN` events automatically drain and destroy temporary session queues on the `<10ms` tactical bus.

---

## Quickstart (Reference Python Library)

```python
import asyncio
from honeycomb_protocol import (
    HoneycombEdge,
    HoneycombEnvelope,
    HoneycombIdempotencyGuard,
    HoneycombPayload,
    SwarmReflexBus,
    compute_target_scope_hash,
)


async def main() -> None:
    bus = SwarmReflexBus()
    guard = HoneycombIdempotencyGuard("honeycomb.db")
    await guard.initialize()

    scope_hash = compute_target_scope_hash(
        garden_target_id="enterprise-retail-surface",
        node_id="product-discovery",
        intent_or_prompt="CONSULTATIVE_GIFT:probe-1",
    )

    envelope = HoneycombEnvelope(
        session_id="session-scout-01",
        source_agent_id="scout-maya",
        target_scope_hash=scope_hash,
        ttl_seconds=300,
        payload=HoneycombPayload(
            node_id="product-discovery",
            node_type="surface_stage",
            edges=[
                HoneycombEdge(
                    source="START",
                    target="product-discovery",
                    relation="OPENED_CONSULTATIVE_PROBE",
                    confidence=0.95,
                )
            ],
            confidence_score=0.95,
            properties={"capabilities": ["graph_probe", "stigmergic_trace"]},
        ),
    )

    # 1. Dynamic Handshake (SESSION_INIT -> HANDSHAKE_ACK)
    await bus.initiate_session(envelope)
    await bus.acknowledge_handshake(envelope, responder_agent_id="queen")

    # 2. Pre-Inference Atomic Idempotency Lock
    claim = await guard.check_and_claim(envelope)
    if claim.discarded:
        return  # Duplicate blocked before spending a single LLM token

    # 3. Two-Phase Verification (Physical Transport + Semantic Graph)
    await bus.verify_double_ack(envelope, physical_transport_ok=True)
    await guard.mark_completed(envelope)

    # 4. Formal Session Teardown (destroys temporary session queues)
    await bus.teardown_session(envelope, reason="COMPLETED")


asyncio.run(main())
```
