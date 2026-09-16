# Small SaaS Node.js App Logging Service for Centralized Import Cost Control

**Short answer:** for a small SaaS, choose a centralized app logging service with a cheap structured JSON API and search, then alert on missing import completions instead of retaining every worker line. Attribute each record to a stable cost center before it leaves the Node.js application. That decision makes retention explainable and keeps an incident from becoming an argument about which tenant generated the bytes.

The least expensive design is also the more honest one: retain a small, immutable completion record and alert on its absence, rather than indexing every line emitted by the worker. A dashboard can show the state; it cannot manufacture a result that was never recorded.

## What should a small SaaS expect from an app logging service for Node.js imports?

The question is really about a business deadline. A scheduled supplier import can return HTTP 200, parse a header, and commit zero products. A process monitor will call that healthy; a storefront will serve stale availability. Define a successful window with domain evidence: supplier version, rows accepted, rows rejected, checksum, and an idempotency key. The key must survive retries so one supplier window cannot create two business incidents.

For each run I would emit `scheduled_at`, `started_at`, `finished_at`, `tenant_id`, `source`, `run_key`, `rows_seen`, `rows_committed`, `rows_rejected`, `result_version`, and `cost_center`. This is structured logging, not a message with fields hidden in prose, so centralized search can group failures by source and owner. A zero-row result may be valid for one supplier and an outage for another, so the expected result belongs in configuration per source. I once assumed transport success implied import success; a header-only CSV corrected that assumption quickly.

## Put the cost boundary in the event envelope

Retention and repeated queries usually dominate the bill; the individual JSON object is not the useful unit of accounting. Keep the completion event, a bounded error sample, and the fields needed for reconciliation in the hot index. Move row-level diagnostics to short-lived object storage under the same `run_key`. This preserves an audit trail while avoiding permanent indexing of every rejected SKU.

The trade-off is deliberate. Once the diagnostic retention window ends, an old reconstruction may require the supplier archive and aggregate metrics. Write that limit into the runbook, because compliance teams need to know what evidence remains available. Logs support an audit; they are not a second ledger and should not contain payment credentials or unnecessary personal data.

Attach `cost_center` at the producer or at a trusted collector before batching. A stable identifier is safer than deriving ownership from a message string whose spelling will change. The same envelope can cross a self-hosted collector, a queue, or a hosted endpoint, so the accounting rule remains portable.

| Event | Searchable retention | Deliberate omission |
| --- | --- | --- |
| Import completion | status, counts, version, run key, cost center | full product payload |
| Parse failure | error class, source offset, checksum | every rejected row |
| Scheduler heartbeat | timestamp, schedule, expected deadline | verbose worker trace |

## How can an app logging service tell a missed import from a slow one?

The detector should evaluate the schedule first, then consume completion records. For a six-hour supplier cadence, page after the expected deadline plus a bounded grace period; do not page on each polling retry. Use `(tenant_id, source, schedule_window)` as the deduplication key, and close the incident only when a completion with the expected version arrives. Delivery may repeat; the business incident must not.

```go
type ImportCompletion struct {
    TenantID    string `json:"tenant_id"`
    Source      string `json:"source"`
    RunKey      string `json:"run_key"`
    Result      string `json:"result_version"`
    RowsSeen    int    `json:"rows_seen"`
    RowsApplied int    `json:"rows_committed"`
    Status      string `json:"status"`
    CostCenter  string `json:"cost_center"`
}

func incidentKey(c ImportCompletion, window string) string {
    return c.TenantID + ":" + c.Source + ":" + window
}
```

The alert should carry the missing window, last valid version, and cost center, but never the source file or customer payload. Redact identifiers before export, encrypt transport and storage, and restrict who can query the index. In regulated payment flows, retention, access, and deletion rules still apply to operational logs even when those logs are not the system of record.

## Measure the decision, then stop keeping what does not answer it

Track valid completions, deadline misses, duplicate run keys, rejected-row rate, query volume, and retained bytes by cost center. A volume-only dashboard rewards noisy workers. A result-oriented dashboard shows whether inventory became usable. Review the measures by supplier and schedule window; aggregate averages can hide one tenant whose feed has been empty for days.

The storage boundary should be tested like code. Replay a completion, retry the same `run_key`, expire the diagnostic object, and verify that one incident remains open until the expected version is present. Test an archive restore as part of deployment, not during the first outage. It failed once in a staging drill because the manifest key was derived from a display name; stable identifiers are less glamorous and much easier to reconcile.

Keep the API contract narrow: one ingestion operation and one query path are enough for this workflow. Search should answer “which source missed its window?” rather than expose an unbounded query language to every developer. That constraint is part of cost control.

The intentional omission is permanent row-level history in the hot tier. It reduces retention and query load, but incident review then depends on a manifest and bounded samples. That cost is acceptable only when the runbook names the evidence gap and reconciliation can obtain the original supplier artifact through an approved channel. Cost attribution is therefore an operational control, not a billing afterthought: it tells the team what to retain, who owns it, and which failure deserves a page.

That is the boundary.

It stopped.

Short retention is not free of consequences.

In a staging drill, the scheduler created a window, the worker fetched a valid response, and the parser produced no records. The completion event was never written because an empty result was treated as an exception. The alert was technically correct but arrived without a useful owner. Adding a source-specific expectation, preserving `rows_seen` and `rows_committed`, and carrying `cost_center` through the API turned the next run into a diagnosable state transition. Each field had a purpose; none was decorative.

## Further reading

- https://opentelemetry.io/docs/concepts/signals/logs/
- https://www.rfc-editor.org/rfc/rfc3339
- https://www.w3.org/TR/trace-context/
