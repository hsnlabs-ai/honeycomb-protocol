# Honeycomb Agent Coordination Protocol (HACP) — Formal Specification v1.0.0

- **Document ID:** `HACP-RFC-0001`
- **Version:** `1.0.0`
- **Status:** `Standard Track (Open Specification)`
- **Maintainer:** `hsnlabs` (`https://hsnlabs.ai`)
- **License:** `MIT`
- **Reference Implementation:** [`agents_swarm/honeycomb/`](../../agents_swarm/honeycomb/__init__.py)

---

## 1. Scope & Architectural Positioning

The **Honeycomb Agent Coordination Protocol (HACP)** defines a deterministic, transport-agnostic, **stigmergic coordination layer** for multi-agent systems operating over long-horizon, intermittent, or adversarial execution environments.

HACP explicitly **does not** replace:
- **Low-level network transports** (`HTTP/2`, `gRPC`, `WebSockets`, `JSON-RPC`), or
- **Agent-to-tool context protocols** such as the **Model Context Protocol (MCP)**, which binds a single model instance to external tools and data resources.

Instead, HACP operates at the **semantic decentralized coordination layer** above agent runtimes and tool interfaces. Rather than coordinating agents via unstructured, point-to-point natural language conversations ($O(N^2)$ message complexity), Honeycomb coordinates autonomous agents via **digital stigmergy**: agents read and write strongly-typed, causally-linked graph primitives and atomic idempotency claims onto a shared state ledger (reference implementation: local **SQLite in Write-Ahead Logging (`WAL`) mode** paired with an in-process `<10ms` tactical reflex bus).

```
+-------------------------------------------------------------------------+
|                    AUTONOMOUS SWARM APPLICATION LAYER                   |
|        (Queen Supervisor, Field Scouts, Domain Auditors, Verifiers)     |
+-------------------------------------------------------------------------+
                                    |
                                    v
+=========================================================================+
|          HONEYCOMB AGENT COORDINATION PROTOCOL (HACP v1.0.0)            |
|  [Pillar 1] Dynamic Capability Handshake (SESSION_INIT / HANDSHAKE_ACK) |
|  [Pillar 2] Strict Envelope + Open Graph Primitives (HoneycombEnvelope) |
|  [Pillar 3] Pre-Inference Atomic Idempotency & Double-ACK               |
|  [Pillar 4] Stateful Resume Tokens (hcrt1) & Formal Session Teardown    |
+=========================================================================+
                  |                                     |
                  v                                     v
+-----------------------------------+   +---------------------------------+
|   SHARED STIGMERGIC LEDGER        |   |   AGENT-TO-TOOL / TRANSPORT     |
|   SQLite WAL (BEGIN IMMEDIATE)    |   |   MCP, GenAI SDK, HTTP, WAHA    |
+-----------------------------------+   +---------------------------------+
```

---

## 2. The Four Architectural Pillars

### 2.1. Pillar I — Dynamic Capability Handshake (`HoneycombHandshake`)

Before any agent executes a probe or invokes an LLM, it MUST establish a mutually verified logical session via the tactical bus (`SwarmReflexBus`). HACP eliminates rigid, hard-coded agent roles in favor of **dynamic capability advertisement and scoped authorization**:

1. **`HONEYCOMB_SESSION_INIT`**:
   - Emitted by the initiating agent (`source_agent_id`).
   - Carries a `HoneycombEnvelope`, the target scope signature (`target_scope_hash`), and an array of active `advertised_capabilities` (e.g., `["graph_probe", "stigmergic_trace", "whatsapp_transport"]`).
2. **`HONEYCOMB_HANDSHAKE_ACK`**:
   - Emitted by the coordinator or peer validator (`responder_agent_id`).
   - Confirms registration of the session and issues `authorized_scopes: list[str]` along with a deterministic cryptographic capability grant token (`scoped_grant_token`, prefixed `hcgrant1.<sha256_prefix>`).

```
Initiating Agent (Scout)                        Coordinator / Ledger (Queen)
       |                                                     |
       |--- 1. HONEYCOMB_SESSION_INIT ---------------------->|
       |       (session_id, target_scope_hash,               |
       |        advertised_capabilities)                     |
       |                                                     |
       |<-- 2. HONEYCOMB_HANDSHAKE_ACK ----------------------|
       |       (authorized_scopes, scoped_grant_token)       |
       |                                                     |
       |=== 3. Atomic Claim (BEGIN IMMEDIATE) ==============>|
       |       [Pre-Inference Idempotency Lock Acquired]     |
       |                                                     |
       |--- 4. HONEYCOMB_DOUBLE_ACK ------------------------>|
       |       (physical_transport_ack=true,                 |
       |        semantic_graph_ack=true)                     |
       |                                                     |
       |--- 5. HONEYCOMB_SESSION_TEARDOWN ------------------>|
       |       (reason="COMPLETED"|"PAUSED",                 |
       |        resume_token="hcrt1...",                     |
       |        deallocated_queues=k)                        |
```

---

### 2.2. Pillar II — Strict Envelope with Open Graph Primitives (`HoneycombEnvelope`)

All control and discovery payloads exchanged across the swarm MUST be encapsulated in a validated `HoneycombEnvelope` (`extra="forbid"` at the envelope and graph structure boundary, while preserving an open `properties` dictionary inside the payload for unknown target surface telemetry).

#### 2.2.1. Control Header (`HoneycombEnvelopeHeader`)

| Field | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `protocol_version` | `str` | Default `"1.0.0"`, non-empty | Semantic version of HACP. |
| `correlation_id` | `str` | RFC 4122 `UUID v4` | Causal trace identifier linking multi-hop events. |
| `session_id` | `str` | Non-empty string | Logical execution session identifier. |
| `source_agent_id` | `str` | Non-empty string | Emitting agent identity (`"queen"`, `"scout-001"`, etc.). |
| `target_scope_hash` | `str` | 64-char hex `SHA-256` | Canonical target scope signature for idempotency locking. |
| `ttl_seconds` | `int` | `> 0` (Default `300`) | Lease duration of the envelope and its idempotency lock. |
| `resume_token` | `str \| None` | Optional `hcrt1.<b64>.<sig>` | Signed continuation token for multi-day mission resumption. |
| `timestamp` | `str` | ISO 8601 UTC string | Creation timestamp normalized to UTC (`+00:00`). |

#### 2.2.2. Graph-Primitive Payload (`HoneycombPayload` & `HoneycombEdge`)

Every observation or state transition is represented as a subgraph mutation:
- `node_id: str` — Identifier of the current or newly discovered surface state node.
- `node_type: str` — Semantic classification (`"surface_stage"`, `"auth_wall"`, `"scout_execution_turn"`, `"queen_campaign_wave"`).
- `edges: list[HoneycombEdge]` — Directed transitions observed:
  - `source: str` (non-empty)
  - `target: str` (non-empty)
  - `relation: str` (non-empty, e.g., `"PROBED_VIA_CONSULTATIVE_GIFT"`, `"BLOCKED_BY_CPF_GATE"`)
  - `confidence: float` ($\in [0.0, 1.0]$)
- `confidence_score: float` ($\in [0.0, 1.0]$) — Aggregate epistemic confidence of the payload.
- `properties: dict[str, Any]` — Open schema extension map carrying raw target telemetry, latency metrics, tool fingerprints, or unstructured surface artifacts.

#### 2.2.3. Canonical Wire Example

```json
{
  "protocol_version": "1.0.0",
  "correlation_id": "9b2f4d3c-6e12-4a88-91c2-71c9d083a210",
  "session_id": "session-scout-bia-w04",
  "source_agent_id": "scout-bia",
  "target_scope_hash": "8f31c49a0e42d9a1573b21c8e04d79b120f6a8d3c44b91e072a1c3f9e5d401b2",
  "ttl_seconds": 300,
  "resume_token": null,
  "timestamp": "2026-11-02T15:30:00+00:00",
  "payload": {
    "node_id": "identification",
    "node_type": "auth_boundary_probe",
    "edges": [
      {
        "source": "product-discovery",
        "target": "identification",
        "relation": "ROUTED_TO_CPF_AUTH_WALL",
        "confidence": 0.96
      }
    ],
    "confidence_score": 0.94,
    "properties": {
      "bandit_arm": "GUEST_POLICY_RULES",
      "rtt_ms": 9812,
      "split_brain_detected": true,
      "raw_surface_hint": "search_catalog hard-interrupts unauthenticated guest session"
    }
  }
}
```

---

### 2.3. Pillar III — Governance, Pre-Inference Atomic Idempotency & Double-ACK

#### 2.3.1. Canonical Scope Hashing (`compute_target_scope_hash`)

To prevent two agents from simultaneously exploring the same target branch or repeating a completed inference call, HACP computes a canonical `SHA-256` digest over normalized target coordinates:

$$\text{target\_scope\_hash} = \operatorname{SHA256}\!\Big(\operatorname{norm}(\text{target\_id}) \,\|\, \text{"|"} \,\|\, \operatorname{norm}(\text{node\_id}) \,\|\, \text{"|"} \,\|\, \operatorname{norm}(\text{intent}) \,\|\, \text{"|"} \,\|\, \operatorname{norm}(\text{extra\_scope})\Big)$$

where $\operatorname{norm}(x)$ lowercases, trims, and collapses internal whitespace.

#### 2.3.2. SQLite WAL Atomic Lock (`honeycomb_idempotency_keys`)

Before any LLM call (`GenAIGateway`) or external target probe is executed, the agent MUST invoke `HoneycombIdempotencyGuard.check_and_claim(envelope)`.

```sql
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
```

The claim executes inside a `BEGIN IMMEDIATE` transaction on SQLite WAL:
- If `status IN ('CLAIMED', 'COMPLETED')` (and the lease has not expired when `expire_stale=True`), the transaction rolls back immediately and returns `IdempotencyClaimResult(acquired=False, discarded=True)`.
- The agent **discards execution at zero inference token cost**.
- Otherwise, the key is atomically inserted/updated to `'CLAIMED'` and transitioned to `'COMPLETED'` via `mark_completed()` once verified.

#### 2.3.3. Two-Phase Verification (`HONEYCOMB_DOUBLE_ACK`)

Transport-level delivery receipts (e.g., HTTP `200 OK` or WhatsApp `[DELIVERED]` hooks) frequently mask downstream silent drops, rate-limit filters, or malformed state transitions. HACP mandates a two-phase acknowledgement (`HONEYCOMB_DOUBLE_ACK`):

$$\text{double\_ack\_verified} \iff \text{physical\_transport\_ack} \;\land\; \text{semantic\_graph\_ack}$$

where:
1. **`physical_transport_ack`**: Confirms the physical channel delivered the probe and received a non-empty response frame within the timeout budget.
2. **`semantic_graph_ack`**: Validates via `envelope.validate_semantic_graph()` that the resulting payload contains a non-empty `node_id`, `node_type`, valid confidence bounds $[0.0, 1.0]$, and well-formed `HoneycombEdge` transitions before committing the state transition to the stigmergic ledger.

---

### 2.4. Pillar IV — Asynchronous Session Lifecycle, Resume Tokens & Formal Teardown

#### 2.4.1. Cryptographically Signed Resume Tokens (`hcrt1`)

Long-horizon missions spanning days or weeks face quota exhaustion, network partitions, or scheduled maintenance windows. When a session or mission pauses, HACP serializes the execution cursor into a self-contained, integrity-signed token:

$$\text{token} = \texttt{"hcrt1."} \,\|\, \operatorname{Base64URL}(\text{canonical\_json}) \,\|\, \texttt{"."} \,\|\, \operatorname{SHA256}(\text{canonical\_json})_{[0:16]}$$

The embedded JSON payload contains:
- `v`: `"1.0.0"`
- `mission_id`: Mission identifier
- `session_id`: Active session identifier
- `turn_index`: Last committed turn index
- `target_scope_hash`: Active scope hash at pause boundary
- `issued_at`: ISO 8601 UTC timestamp
- `nonce`: 12-hex-character entropy nonce
- `state`: Optional dictionary snapshot of graph state variables

Upon resumption (`parse_resume_token`), the runtime verifies the 16-hex SHA-256 signature prior to rehydrating the state machine. Any bit modification triggers a deterministic `ValueError("Resume token integrity signature mismatch.")`.

#### 2.4.2. Formal Dialogue Teardown (`HONEYCOMB_SESSION_TEARDOWN`)

Every session MUST conclude with an explicit `HONEYCOMB_SESSION_TEARDOWN` event carrying `reason` (`"COMPLETED"`, `"PAUSED"`, `"IDEMPOTENCY_SKIPPED"`, or `"ABORTED"`). Executing teardown automatically:
1. Marks the session record as `torn_down = True`.
2. Drains and deallocates all ephemeral `asyncio.Queue` instances bound to `session_id` via `SwarmReflexBus.destroy_session_queues(session_id)`, preventing memory leaks and zombie listener accumulation.
3. Persists the final `resume_token` if `reason == "PAUSED"`.
