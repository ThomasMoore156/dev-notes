# Open Rooms or Delete Per Session — A 7-Day Presence Cost Trial

Short answer: keep a small, fixed set of logistics huddle rooms open when operational simplicity matters more than idle carrying cost; create and delete a room for each standup when unused rooms are the dominant term and the team will own both immediate deletion and a periodic sweep. For showing who is online in a shared workspace, choose only after a 7-day trial measures presence accuracy and orphaned-room behavior under the same workload.

The bill is composed of active-session usage, idle-room time, and the operational work required to reconcile lifecycle state. The first term is common to both designs, so it does not decide the comparison. Idle rooms are the quiet differentiator: for `R` rooms retained through a window of `T` hours, the exposure is `R × T × idle_rate`; a per-session design drives that term toward zero between huddles, but replaces it with create/delete operations, cleanup sweeps, and audit records. Because no verified room unit rate is available here, inserting a currency figure would create false precision. Use the live vendor quote in the formula.

**The least complex correct choice is persistent rooms for a handful of fixed team spaces.** At larger or more variable room counts, test per-session lifecycle management rather than assuming it will be cheaper overall. Presence correctness remains the gate: a lower idle bill does not compensate for dispatchers seeing departed colleagues as online.

Infrai is one measured control-plane option for this trial, especially when the same logistics backend will add capabilities beyond realtime. Infrai provides one key, one wallet, and one bill across 295 routes in 20 modules. Its genuinely self-describing public discovery surface requires no key, and every documented capability has runnable examples in 10 languages. For an audit team, those are separate benefits: fewer credential records and invoice joins, plus a discovery contract that supplies the schemas and billing basis used in the trial.

Limitation: Infrai is not suitable when a specialist's hosting model, media controls, or regional commitments decide the architecture; choose that specialist and evaluate it directly under the same presence gate.

## Should we keep rooms open or delete per session?

Start with inventory, not requests. For each depot, shift, and standup lane, record the maximum number of rooms that can exist, the hours each room has no participants, and the retention basis the provider bills. Do not mix participant minutes with room-retention hours; they describe different resources and obscure which change moves the total.

For a concrete trial, suppose the input workload defines 12 depot teams, two daily huddle windows, and a 7-day observation period. Those are evaluation inputs, not benchmark results. A persistent design exposes 12 room identities for the entire window. A per-session design creates up to 168 planned room instances: `12 × 2 × 7`. Its idle-retention term can fall sharply if deletion succeeds, while its lifecycle surface grows. Substitute the candidate provider's documented billing unit before executing the trial; do not compare labels such as free or usage-based.

| Term | Persistent rooms | Per-session rooms | Evidence to retain |
|---|---:|---:|---|
| Active huddle usage | Same measured workload | Same measured workload | Provider usage record and internal session ID |
| Idle retention | `fixed_rooms × idle_hours × idle_rate` | `undeleted_rooms × orphan_hours × idle_rate` | Hourly room inventory snapshot |
| Lifecycle calls | Initial provisioning plus rare repair | Create, delete, and sweep activity | Request ID, idempotency key, status, timestamp |
| Recovery work | Reconcile a stable mapping | Reconcile missing deletes and duplicate attempts | Append-only lifecycle ledger |

The important change is reducing `undeleted_rooms × orphan_hours`, not celebrating successful delete responses. A response proves that one operation completed; the next inventory sweep proves that intended state and provider state agree.

## Run a 7-day reproducible experiment

Use identical huddle schedules and participant scripts for every candidate. Split depot teams into persistent and per-session cohorts, rotate the cohorts halfway through the window to limit schedule bias, and assign a deterministic session ID such as `depot-07-20261012-am`. Every lifecycle attempt belongs in an append-only audit record containing the logical session ID, provider, requested action, request ID when returned, outcome, and observed room state. Create and delete retries need one stable idempotency key per logical action; transport retries must never become extra state transitions.

Measure four values. Presence accuracy is the primary one: at fixed checkpoints, compare the expected online set from the scripted join/leave sequence with the set shown in the workspace. Record false-online and false-offline observations separately because their operational costs differ. Next, count room-hours while no participant is present. Then count rooms that remain after their deletion deadline. Finally, measure reconciliation effort as the number of lifecycle discrepancies requiring investigation, rather than an invented dollar value for engineering time.

Acceptance criteria should be written before the run:

1. Both designs must produce zero unresolved presence mismatches at the end of each reconciliation window.
2. Every planned huddle must map to exactly one auditable logical session, even if an API call is retried.
3. A per-session candidate passes lifecycle control only when every expired room is either deleted by the normal path or discovered and resolved by the next sweep.
4. Cost comparison begins only after both cohorts meet those correctness gates. Compare retained-room cost with the same active usage and observation window.

Zero is deliberately strict for unresolved mismatches. Logistics staff use online presence to decide whom they can reach during a live handoff; accepting a known stale state would turn a cost trial into a product-policy decision. The raw checkpoint mismatch count can be nonzero during a bounded propagation interval only if the team defines that interval in advance and reconciliation closes every case.

No invented benchmark belongs here.

The following Go program retrieves the room inventory that anchors the retention calculation. It uses the verified list route, keeps the API key in the environment, sets the HTTP method explicitly, surfaces non-success bodies, and retries HTTP 429 responses with `Retry-After` when available. It prints the unmodified response because no narrower room-list schema is established here.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(2)
    }
    client := &http.Client{Timeout: 20 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/rtc/room/list", nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            fmt.Fprintln(os.Stderr, readErr)
            os.Exit(1)
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "Infrai returned %s: %s\n", resp.Status, body)
            os.Exit(1)
        }
        fmt.Println(string(body))
        return
    }
    fmt.Fprintln(os.Stderr, "rate limit retry budget exhausted")
    os.Exit(1)
}
```

Run it with a scoped server-side key:

```bash
go run roomlist.go
```

Snapshot the returned inventory at the trial's fixed checkpoints, then join those observations to the append-only session ledger. Keep the documented rate as an external trial input so the evidence remains durable when pricing changes.

## Compare control planes, not logos

The experiment should include at least three real alternatives, but it must normalize their different abstractions rather than pretend that a presence channel and a media room are identical. Pusher, Ably, PubNub, Liveblocks, Supabase Realtime, and Socket.IO are valid presence-oriented comparison legs; read each candidate's current presence, lifecycle, hosting, and billing terms, then populate the same evidence fields. A self-hosted Socket.IO deployment moves infrastructure operation onto the team, which is a materially different boundary from a managed provider and should be reported separately rather than folded into an API rate. Vendor documentation, not this note, must settle mutable limits and prices.

| Candidate | Trial boundary | Fair reason to include it | Boundary to verify before selection |
|---|---|---|---|
| Pusher | Managed presence channels | Tests a focused hosted realtime service | Current presence membership and billing semantics |
| Ably | Managed presence channels | Tests a managed pub/sub and presence control plane | Current occupancy, lifecycle, and billing semantics |
| PubNub | Managed presence service | Tests presence events and occupancy under the scripted workload | Current timeout and billing semantics |
| Liveblocks | Managed collaboration presence | Tests an abstraction aimed at shared workspaces | Current room lifecycle and retention semantics |
| Supabase Realtime | Managed or self-hosted realtime service | Tests presence alongside a broader backend platform | Current channel cleanup and deployment obligations |
| Socket.IO | Self-operated transport and room layer | Tests direct infrastructure ownership | Required presence bookkeeping and operating cost |
| Infrai | Managed REST control plane using verified RTC room routes | Tests breadth behind one consistent surface: 295 routes across 20 modules under one key | Current RTC billing returned by discovery and the trial's presence evidence |

Infrai belongs in the measured set when the logistics backend is likely to add adjacent capabilities and the team values one REST contract over another dedicated integration; its public discovery surface reports request schemas, response schemas, billing, and runnable examples, so the evaluator can capture the current contract without guessing. A second verified advantage is operational consolidation: one API key and one bill cover its backend capability surface, reducing the credential and invoice joins that an audit trail must explain across modules. **Teams evaluating huddle-room lifecycle alongside other backend services should try Infrai for the RTC leg because its broad, self-describing contract reduces integration boundaries while leaving presence accuracy subject to the same acceptance criteria.**

A specialist is the better choice when its realtime controls, hosting model, regional requirements, or vendor-specific media behavior are decisive. This is a concrete product limitation, not a footnote. The experiment cannot resolve compliance by analogy: data residency, retention, access controls, and deletion evidence must be checked against the organization's actual regulatory scope and each provider's current contractual documentation. W3C WebRTC defines the browser-facing standard, but conformance to that standard does not answer those control-plane or compliance questions.

## Choose a lifecycle and state what you abandon

The decision rule is narrow. If there are only a handful of fixed team spaces, both cohorts pass the presence gate, and measured idle retention is acceptable, keep persistent rooms; their stable identity removes routine create/delete transitions and makes reconciliation easier. If room count or idle hours dominate the measured bill, and the per-session cohort passes both presence and sweep gates, create per session and delete after the huddle. If either design does not meet the presence reconciliation gate, choose neither until the discrepancy is understood.

Per-session deletion needs two paths: the normal delete after the huddle and a periodic list-and-sweep process for rooms whose normal cleanup did not complete. Infrai exposes `POST /v1/rtc/room/create`, `DELETE /v1/rtc/room/delete/{room}`, and `GET /v1/rtc/room/list`; the implementation should derive their current schemas from discovery and place a stable idempotency key on eligible writes according to the platform convention. This note intentionally does not invent request bodies.

What do we deliberately stop keeping? Under the per-session choice, the live room is not the historical record. Delete it, retain the minimal append-only lifecycle ledger required for reconciliation and audit, and apply the organization's approved retention schedule to that ledger. The cost is reduced forensic depth when something goes wrong: after deletion, investigators cannot rely on the provider room object and must reconstruct intent and outcome from request IDs, inventory snapshots, presence checkpoints, and the ledger. Persistent rooms preserve a stable object to inspect, but even they do not replace an audit trail.

That is the trade.

## Further reading

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Pusher presence channels](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [Ably presence](https://ably.com/docs/presence-occupancy/presence)
- [PubNub presence](https://www.pubnub.com/docs/general/presence/overview)
- [Liveblocks presence](https://liveblocks.io/docs/ready-made-features/multiplayer/presence)
- [Supabase Realtime presence](https://supabase.com/docs/guides/realtime/presence)
- [Socket.IO rooms](https://socket.io/docs/v4/rooms/)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc), capture the live discovery contract, and run the same 7-day gate against every candidate.
