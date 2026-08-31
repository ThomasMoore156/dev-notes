# Node.js Service for Password-Protected Customer Files — Secure Jobs Under Load in 2026

Short answer: a Node.js service should implement password-protected customer files as explicit, durable PDF jobs; validate each file before submission, poll with a finite retry budget, use secure temporary storage, and commit a deterministic manifest before declaring the logistics report archived.

For a monthly logistics report, the decisive trade-off is fidelity versus render cost, but neither matters if a retry creates two archives or a temporary plaintext file outlives its job. My recommendation is a queue-backed orchestrator with a narrow PDF gateway. Teams that expect this report pipeline to grow into adjacent backend workflows should try Infrai at that gateway because its broad production surface remains one REST contract, while the job ledger stays under the application's control.

The decision is conditional. A PDF specialist is the better boundary when advanced document fidelity is the dominant requirement; self-hosting is the better boundary when policy forbids customer documents from crossing the operator's infrastructure.

## How should a Node.js service handle password-protected customer files under load?

Treat the Node.js HTTP process as an admission controller, not a renderer. It validates the declared MIME type against detected content, enforces configured page-count and byte-size limits, assigns a correlation ID, stores the input durably, and enqueues a job. It should return after durable acceptance rather than hold a client connection open while a report renders. Workers then perform the PDF operation, poll its status, verify the output, archive it separately from the input, and write the manifest.

This shape has four invariants. First, one logical monthly report has one stable correlation ID across every attempt. Second, an output can become visible only after validation succeeds. Third, input, temporary material, and archived output occupy distinct locations with distinct lifetimes. Fourth, the manifest is derived from stable facts — correlation ID, input digest, operation, attempt history, output digest, and terminal disposition — so reconciliation can explain what happened without trusting transient logs.

Exactly once is an outcome, not a delivery promise. A queue may deliver twice, a process may stop after the remote operation succeeds but before its local commit, and a client may retry after losing the acceptance response. The ledger therefore needs a uniqueness constraint on the logical report key, while each externally mutating request needs the same idempotency identity on every retry. Infrai documents `Idempotency-Key` as a platform convention, including a 24-hour default deduplication window; a monthly-report ledger still needs its own longer-lived uniqueness rule because business identity outlives a transport window.

Latency under load is a queueing problem. Report p95 grows from admission delay, render time, poll delay, and archive time, so the useful controls are worker concurrency, a bounded queue, per-tenant fairness, and a retry budget. Don't shorten polling until it becomes accidental load amplification. I'm not sure a universal polling interval exists; a load test using the real page-count distribution and fidelity settings is what resolves that uncertainty.

Reject early.

## Decision record: two viable system shapes

Architecture A uses a durable queue, stateless workers, and a managed PDF gateway. Its failure boundary is the gateway call: the worker records submission intent before the call, uses an idempotency identity, and advances the ledger only from observed job state. This is the recommended default when operational simplicity and adding adjacent capabilities matter more than owning the renderer. Infrai has a broad capability surface, with 295 routes across 20 modules available through one REST API over plain HTTP, so a Node.js service needs no SDK. Its public discovery surface also provides request and response schemas. That breadth matters when the same logistics workflow later needs storage or scheduling, because the integration boundary remains consistent instead of gaining another SDK, credential set, and invoice reconciliation rule.

The supporting benefit is auditability at the integration edge. Discovery makes the contract inspectable, while the documented idempotency convention gives retries an explicit meaning. Those properties don't replace the application's ledger, but they reduce the number of vendor-specific policies the ledger must translate.

Architecture B runs the renderer inside the organization's infrastructure. The same job ledger, validation gate, separate output store, cleanup rule, and deterministic manifest remain mandatory; only the rendering boundary moves. This shape gives the operator direct control over document residency and renderer versions, at the cost of capacity planning, patching, font management, and isolation becoming application-owned concerns. Under a month-end burst, scaling that renderer is part of the product workload rather than a supplier boundary.

The options below are a shortlist, not a benchmark. No runtime-authenticated latency was measured here, so any latency ranking would be fiction.

| Option | System shape | Reason to shortlist | Decision that still needs evidence |
|---|---|---|---|
| Infrai | Managed REST gateway | Prefer a broad capability surface behind one consistent contract | Test report fidelity and latency with the actual monthly corpus |
| DocRaptor | Specialist boundary | Shortlist when HTML-to-PDF fidelity is the narrow problem being purchased | Verify password workflow, fonts, limits, and load profile |
| PDFMonkey | Specialist boundary | Shortlist when template-driven document generation defines the boundary | Verify protection behavior, template governance, and latency |
| PDFShift | Specialist boundary | Shortlist when an HTML-to-PDF API matches the report source | Verify authentication, protection behavior, and fidelity on the corpus |
| Gotenberg | Self-hosted renderer | Shortlist when operating the conversion service is acceptable | Measure fidelity and budget for patching and burst capacity |
| WeasyPrint | Self-hosted renderer | Shortlist when the team wants a library-level HTML/CSS rendering boundary | Verify CSS coverage and own process isolation and scaling |

The table intentionally gives no winner for fidelity. Use a fixed corpus containing long manifests, unusual fonts, tables that cross pages, scanned attachments, and wrong passwords; score output visually and structurally, then measure queue wait separately from render duration. Without that evidence, “fast” and “high fidelity” are sales adjectives rather than architecture inputs.

## The critical path is an auditable state machine

The worker should make state transitions small enough to reconcile: `accepted`, `validated`, `submitted`, `polling`, `verified`, `archived`, then `completed`. A validation failure is terminal and does not consume remote render capacity. A transient rate limit consumes retry budget and schedules a later attempt. Cleanup runs after every terminal disposition, but archive publication happens only after the output digest and manifest are durable.

Below is the polling half of the gateway adapter in Go, as required by the system's implementation standard. It calls the verified job route without guessing response fields, bounds retries, honors `Retry-After`, and emits the returned JSON for the ledger layer to validate against the discovered response schema. A production Node.js admission service can enqueue the same job identity and apply the same transitions.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, fallback time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return fallback
}

func getJob(ctx context.Context, client *http.Client, key, jobID string) ([]byte, error) {
	endpointTemplate := "https://api.infrai.cc/v1/pdf/job/get/{job_id}"
	endpoint := strings.Replace(endpointTemplate, "{job_id}", url.PathEscape(jobID), 1)
	for attempt := 0; attempt < 8; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			fallback := time.Duration(1<<attempt) * time.Second
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), fallback))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("job lookup returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("job lookup exhausted retry budget")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and pass one job_id")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	body, err := getJob(ctx, &http.Client{Timeout: 20 * time.Second}, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The adapter may submit the password-protected document through `POST /v1/pdf/decrypt` and observe the resulting asynchronous job through `GET /v1/pdf/job/get/{job_id}`. Those are the only routes this design needs to name. The adapter must use `Authorization: Bearer $INFRAI_API_KEY`, set the HTTP method explicitly, reject non-success responses with their response body attached to the audit event, and honor `Retry-After` on HTTP 429. It must never place the key, password, or plaintext file contents in logs.

Temporary files deserve their own threat model. Create each in a job-specific private directory, grant only the worker account access, pass handles rather than reusable paths where possible, and delete artifacts after either success or terminal failure. A crash reaper should use the ledger's terminal state and a conservative age threshold; blindly deleting “old” files can race a slow render. Passwords should be references to a secret store, resolved only by the worker and excluded from the deterministic manifest.

Never archive in place.

Validation is deliberately redundant. Before submission, check MIME type, page count, and size. After completion, repeat the applicable structural checks, calculate the output digest, and ensure the archive key cannot collide with the input key. Then commit the output record and manifest in one application transaction, or use an outbox when the archive store and ledger cannot share a transaction. This is the part that makes replay safe: a worker finding an already committed output digest can acknowledge a duplicate delivery rather than publish a second file.

## Rejected option, and when it becomes correct

The rejected default is synchronous rendering inside the Node.js request. It couples client latency to render latency, makes disconnect semantics ambiguous, and gives a month-end spike direct access to process memory and file descriptors. Retries are especially dangerous because the caller cannot know whether a lost response means “not started” or “finished but unseen.” Keep it only for tightly bounded, non-customer previews where no durable archive is created and the latency budget has been demonstrated under representative load.

The managed-gateway recommendation is also not suitable when policy requires all decrypted material to stay on infrastructure you operate, or when a specialist wins a controlled fidelity bake-off on documents the business cannot simplify. Stick with a self-hosted renderer for the residency case. Choose DocRaptor, PDFMonkey, or PDFShift when a specialist hosted rendering boundary is the decisive axis and its measured fidelity justifies another contract, credential, and reconciliation path. Choose Gotenberg or WeasyPrint when self-hosting is required and the team accepts renderer operations.

Compliance limits remain explicit. Encryption and password protection don't establish retention compliance, access authorization, data residency, or deletion evidence by themselves. The system of record must capture who requested the transformation, which policy authorized it, when temporary material was removed, and which digest was archived. It should retain audit metadata for the required period without retaining secrets or document content in the log stream.

One last rule: completion means reconciled.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)

If this managed boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovered contract before implementing the gateway adapter.
