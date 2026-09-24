# Architecture Diagrams

Current architecture (single-process monolith, one event loop, one SQLite, one
broker connection — correct for a single account). ★ marks subsystems added in
the 2026-06 reliability hardening pass (see `docs/OBSERVABILITY.md`).

## 1. Architecture diagram

```mermaid
flowchart TB
  subgraph External
    DISC[Discord analyst channels]
    BROK[Brokers: Public.com REST / IBKR ib_async]
    GEM[Gemini Vertex AI]
  end

  subgraph Process["relaybot / ibark — single Python process, one event loop"]
    direction TB
    FWD[forwarder] --> LIS[discord_listener.on_message]
    LIS --> PARSE[parser regex → ai_parser fallback]
    PARSE -. miss .-> GEM
    PARSE --> PERSIST[_persist_alert<br/>UNIQUE discord_message_id ★]
    PERSIST --> QUEUE[[order_queue<br/>PriorityQueue · exits jump BUYs ★]]
    QUEUE --> WORKER((single serial worker ★))
    WORKER --> EXEC[PublicExecutor / IBKRBroker.execute]
    EXEC --> GUARD[guardrails.check_guardrails<br/>off-loop via to_thread ★]
    GUARD --> SIZE[kelly_sizer / vix_sizer]
    SIZE --> EXEC
    EXEC --> BROK
    EXEC --> POLL[event-driven fills]
    POLL --> MON[position_monitor poll loop<br/>PT/SL/TPT exits]
    MON --> BROK
    REC[reconciler gate] --> DB
    GUARD <--> DB[(SQLite WAL<br/>busy_timeout · wal_autockpt ★<br/>+ composite indexes ★)]
    EXEC <--> DB
    MON <--> DB
    GUARD -. read/write .-> SS[(system_state<br/>persisted halt ★)]
    MET[metrics ★] -. /metrics ★ .-> OBS[Prometheus/Grafana]
    LAG[event-loop-lag sampler ★] --> MET
  end

  GUARD -.->|pass/block by reason ★| MET
  QUEUE -.->|depth / fallback ★| MET
  EXEC -.->|orders placed ★| MET

  classDef new fill:#e6ffe6,stroke:#2a2;
  class QUEUE,WORKER,MET,SS,LAG new;
```

## 2. Sequence diagram (alert → order-ack)

```mermaid
sequenceDiagram
  autonumber
  participant D as Discord
  participant L as Listener
  participant DB as SQLite
  participant Q as order_queue ★
  participant W as serial worker ★
  participant G as guardrails
  participant SS as system_state ★
  participant B as Broker
  participant M as metrics ★

  D->>L: alert (BUY/SELL …)
  L->>L: parse (regex → AI fallback)
  L->>DB: INSERT alert (UNIQUE msg-id) ★
  alt duplicate message
    DB-->>L: IntegrityError → drop ★
    L->>M: alert_dup_rejected_total++
  else new
    L->>M: alerts_received_total++
    L->>Q: dispatch(priority = EXIT if SELL else ENTRY) ★
    Note over Q,W: SELLs drain ahead of BUYs; one job at a time
    Q->>W: dequeue
    W->>G: check_guardrails (to_thread, off loop) ★
    G->>SS: ensure_halt_loaded (restart-safe) ★
    G->>DB: open-count / daily-trades / daily-PnL<br/>(stale-mark haircut ★)
    alt blocked
      G->>M: guardrail_block_total{reason}++ ★
      opt MAX LOSS
        G->>SS: persist halt ★
        G->>M: halt_set_total++ ★
      end
      G-->>W: (False, reason) → no order
    else allowed
      G->>M: guardrail_pass_total++ ★
      W->>B: place_order (orderRef=oid)
      B-->>W: ack PENDING
      W->>DB: INSERT order
      W->>M: orders_placed_total{side}++ ★
      B-->>M: (later) event-driven fill → position
    end
  end
```

## 3. Data flow / trust boundaries

```mermaid
flowchart LR
  subgraph Untrusted["Untrusted input"]
    A[Discord alerts]
  end

  subgraph Trusted["Single process — serialized, idempotent"]
    P[parse + validate]
    G[risk + guardrails<br/>persisted halt ★ · stale-mark guard ★]
    O[serial order worker ★<br/>exit-priority ★]
  end

  subgraph StateStore["SQLite WAL"]
    T1[(alerts<br/>UNIQUE msg-id ★)]
    T2[(positions)]
    T3[(orders)]
    T4[(discord_signals)]
    T5[(system_state ★)]
  end

  subgraph Truth["Source of truth"]
    BK[Broker accounts]
  end

  subgraph Telemetry
    MX[/metrics ★/]
  end

  A -->|no HMAC; Discord-authed| P
  P --> G --> O
  O -->|creds in env/config.ini| BK
  O --> T2 & T3
  P --> T1
  G <--> T5
  BK -->|reconcile-on-boot gate| T2
  T1 -.retention prune ★.-> X[archive/delete]
  G & O -.->|counters/gauges ★| MX

  classDef new fill:#e6ffe6,stroke:#2a2;
  class T5,MX new;
```

## Notes

- **Single event loop:** listener, position monitor, forwarder, reconciler, and
  the order worker are all `asyncio` tasks — no OS threads except the transient
  `to_thread` the guardrail runs in.
- **Serialized order path:** every order flows through ONE worker draining a
  priority queue, so the guardrail count→insert is effectively atomic (no
  cap-breach race) and exits drain ahead of entries.
- **Source of truth is the broker:** local SQLite is a cache; reconcile-on-boot
  reconciles positions before BUYs are allowed.
- **Open gaps** (not shown, tracked): deterministic broker `orderRef`, durable
  crash-recovery queue, in-flight-order journal, cross-host shared state
  (Postgres). See `docs/OBSERVABILITY.md`.
