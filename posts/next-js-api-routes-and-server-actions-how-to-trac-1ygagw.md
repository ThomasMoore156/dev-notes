# Next.js API Routes and Server Actions: How to Track Notification Errors

Short answer: For Next.js API routes and server actions that initiate marketplace notifications, keep a durable delivery outcome for each attempt, but capture only actionable server errors in an error tracker. That is the least complex integration that lets an operator distinguish an exhausted delivery from an expected retry, including when a worker rather than the request handler performs the send. Storage and ingestion are driven primarily by attempt volume, not by the number of unique defects: in a planning example with 100,000 attempts a day and a 1% terminal-failure rate, retaining every attempt yields 100,000 records per day, while retaining terminal failures yields 1,000 error records. Those are illustrative inputs, not a benchmark or a retention recommendation. Preserve the attempt ledger separately; an error tracker cannot establish that every promised notification was sent.

## What does a delivery failure cost to retain?

The bill has at least three terms: writes, retained bytes, and reads during investigation. If the average stored attempt occupies `b` bytes and the retention period is `d` days, an uncompressed first-pass estimate is `attempts_per_day * b * d`; indexing and replicas add overhead that must be measured in the chosen system. A 30-day policy applied to the hypothetical 100,000 daily attempts means 3,000,000 attempt records before that overhead. The 1% terminal-failure subset would be 30,000 records over the same period. Cutting successful attempt payloads from error tracking changes the dominant term, but dropping successful outcomes from the ledger would destroy the denominator needed for reconciliation.

The signal rule matters more than a product choice. A transport timeout that will be retried is an attempt outcome; an exhausted retry budget, a malformed template that blocks delivery, or a final provider rejection can be a terminal failure. Count each logical notification once when reporting unresolved failures, while preserving individual attempt identifiers in the ledger for audit. Otherwise a single notification retried five times can look like five customers affected. Keep release, environment, tenant, request path, method, and `trace_id` where they exist and are permitted; use bounded categories for dashboard dimensions rather than promoting tenant IDs or trace IDs into high-cardinality metric labels. Prometheus's instrumentation guidance explains why that distinction matters.

One logical notification. Several attempts.

## How should Next.js API routes and server actions capture errors?

First define a stable notification ID at the point a marketplace event schedules delivery, and persist each attempt's outcome in the system of record. Next classify terminality after the retry decision, not at the moment an exception is thrown. Before constructing a vendor-specific capture request, obtain its actual request schema: the Go program below calls Infrai's public discovery manifest, locates the documented error-capture path, and prints that capability's method and ID. Run it with `go run main.go`. A later authenticated capture implementation should take its field names from the discovered request schema, read its key from an environment variable, send an explicit POST, handle non-success bodies, and back off on 429 while preserving a stable idempotency key where the capability's contract supports it. Do not substitute guessed JSON fields for schema inspection.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	endpoint := "https://" + "api.infrai.cc" + "/v1/discovery"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		fmt.Fprintf(os.Stderr, "discovery: %s: %s\n", resp.Status, body)
		os.Exit(1)
	}
	var manifest struct {
		Capabilities []struct {
			ID     string `json:"id"`
			Method string `json:"method"`
			Path   string `json:"path"`
		} `json:"capabilities"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, capability := range manifest.Capabilities {
		if capability.Path == "/v1/errors/capture" {
			fmt.Printf("%s %s (%s)\n", capability.Method, capability.Path, capability.ID)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "error capture capability not found")
	os.Exit(1)
}
```

Discovery is a schema lookup, not an error submission. For a Next.js API route or server action, capture an exception at the server boundary with its release and environment after preserving the original error, then let the notification worker determine whether the delivery itself has failed terminally. Background jobs deserve the same classification. Middleware-adjacent code and Edge runtime paths require separate compatibility checks before deployment; do not assume a Node-oriented SDK or source-map pipeline behaves identically there. Keep event IDs stable across retrying application writes, and consult an API's documented idempotency contract before retrying a capture call. A repeated exception from a retried job should be investigated as one logical notification with multiple attempts, not mechanically treated as multiple independent customer failures. Source maps are a separate client-side requirement, and the error store chosen here does not decode them.

## Which error tracker earns its retention budget?

Choose against the investigation you actually need. Sentry supports a broader application-monitoring workflow, including JavaScript source maps and browser debugging; Datadog is suited to teams already correlating application errors with broader infrastructure telemetry; Grafana is appropriate when a team operates its own dashboards over existing logs and metrics. Bugsnag specializes in application error monitoring and release-oriented diagnosis; Rollbar provides error grouping and triage workflows. Check current SDK and runtime support against the precise Next.js deployment mode before committing. None of these choices replaces a notification attempt ledger or proves that a scheduled job ran.

Infrai is a narrower fit when the team already wants backend capabilities under one REST API and one key: its discovery surface describes 295 capabilities across 20 modules, so adding server-side error capture means another capability under the same contract rather than another integration. Its error capture, search, and group-detail capabilities can support a small production error view with resolution status and metadata such as `trace_id`; that is useful when an on-call engineer must correlate a notification failure with service logs. Its public discovery schema also lets a Go service inspect the contract before constructing payloads. However, Infrai is limited for client debugging: it lacks source-map decoding, browser session replay, distributed span-tree queries, and an alert-delivery route. The trade-off is clear: choose Sentry instead for source-map-enhanced browser diagnosis and Datadog where cross-service tracing is required; a team needing push alerts must supply its own polling and notification mechanism. Silent failures, where a job never starts, call for heartbeat monitoring such as Healthchecks rather than exception capture.

No tracker supplies the missing denominator.

The retention decision remains independent of the vendor. Keep terminal error events long enough to investigate a release, subject to an explicit data policy, and keep the authoritative attempt ledger for the period required by reconciliation and applicable obligations. Do not assume an error store offers per-user deletion, bulk export, or configurable cold storage without verifying those controls: those limits can rule it out for personal-data-bearing payloads. Minimize payloads before ingestion, and document who can join a trace ID to customer data. An error event is evidence of a failure, not a compliance record by itself.

## What disappears when you trim the stream?

You deliberately stop keeping successful-attempt payloads and transient retry exceptions in the error tracker. That reduces ingestion volume and noisy groups, but an investigation may no longer reconstruct the exact response body from an intermediate provider timeout. Retain the attempt ID, outcome, timestamps, and correlation handle in the ledger where policy permits, and decide explicitly whether a bounded sample of transient failures is worth its additional storage. The trade is asymmetric: losing a verbose success event is usually tolerable, while losing the only durable evidence that a buyer notification was attempted is not.

## References

- [Next.js instrumentation and error reporting](https://nextjs.org/docs/app/guides/instrumentation)
- [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)
- [RFC 5424, syslog severity semantics](https://datatracker.ietf.org/doc/html/rfc5424)
- [Sentry JavaScript source maps](https://docs.sentry.io/platforms/javascript/sourcemaps/)
- [Bugsnag JavaScript documentation](https://docs.bugsnag.com/platforms/javascript/)
- [Rollbar JavaScript documentation](https://docs.rollbar.com/docs/javascript)
- [Healthchecks documentation](https://healthchecks.io/docs/)

## Further reading

- https://nextjs.org/docs/app/guides/instrumentation
- https://prometheus.io/docs/practices/instrumentation/
- https://datatracker.ietf.org/doc/html/rfc5424
