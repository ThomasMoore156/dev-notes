# Audit Guided Capture of Server Exceptions and Repeated Error Alerts Through Polling

A property checkout can fail after a payment has already taken effect, so the most consequential trade-off is between detecting repeated exceptions quickly and preserving enough evidence to decide whether a deployment rollback is safe. TL;DR: capture server exceptions centrally, poll their error groups over a recent time window, and alert on repeated failures; keep the payment outcome and reconciliation record separate from the exception count, because an alert cannot establish that a charge needs to be repeated.

For an Express application or a background worker, the capture boundary belongs where an exception becomes an application failure. The alerting decision comes afterward. This note uses a property checkout that collects an outstanding balance at move-out, with rollback safety as the acceptance criterion and an exactly-once mindset applied to business effects rather than assumed of telemetry delivery. The workflow is a design example, not a reported incident.

## How should capture of server exceptions drive an alert on repeated errors?

It proves recurrence.

Suppose a checkout request has a stable operation identifier shared with the payment attempt and the reconciliation record. A timeout leaves that operation's outcome uncertain; a second request with a new business identifier risks creating a second effect, even if the first request was recorded as an exception. The capture record should therefore help an operator find the original operation, its deployment revision, and its reconciliation state, while the payment system remains responsible for determining whether money moved. These are proposed application invariants, not claims that an error service validates payment correctness.

An error group answers a narrower question: is a recognizable failure recurring? Group-based alerting reduces the noise of notifying on every event and gives a junior engineer a tractable incident boundary. However, one group can span several checkout attempts, and one attempt can produce several captured exceptions. Neither group totals nor event totals should become a count of failed charges.

Keep those quantities separate.

The rollback decision needs a further distinction between a reversible application deployment and its potentially irreversible effects. Reverting code may stop a new exception pattern, but it cannot establish the state of a payment already submitted, restore a database representation that older code cannot read, or authorize a compensating financial operation. A useful alert links the error group to a release and a reconciliation procedure; it does not instruct the responder to replay all affected checkouts.

## Derive the capture and polling contract from that constraint

Start with centralized capture of exceptions from the checkout request boundary and the worker that completes its follow-up work. Associate the application's stable checkout operation identifier with its internal diagnostic record, and keep sensitive resident or payment information out of exception messages. A deployment revision is useful evidence for rollback decisions; an arbitrary message containing a resident identifier is a poor grouping policy because changing message text can fragment the same underlying failure. These are instrumentation choices to validate during setup, not promises about a provider's grouping algorithm.

Then poll unresolved groups every few minutes and evaluate recurrence inside an explicit recent window. For an illustrative policy, use completed 5-minute windows and a threshold of 3 captured occurrences, checking every 2 minutes. Those numbers are starting configuration choices. A completed window provides a stable incident key, while waiting for its end introduces delay; a rolling window can react sooner, but requires a separate incident lifecycle to avoid repeatedly notifying as the window advances. Choose that trade-off deliberately.

A lifetime group total is insufficient.

Before implementing the adapter, inspect the selected service's current request and response schemas, establish how event times and group identity are represented, and verify how pagination affects the count. The adapter should produce an application-owned observation containing a group identity, release, bounded interval, and count. If the available response cannot establish the interval count, obtain the necessary event evidence before evaluating the threshold. Do not silently relabel an unresolved group's cumulative count as recent activity, or assume that an undocumented server-side time filter exists.

The poller's progress record, its alert intent, and its notification delivery state also have different meanings. Persisting progress before an alert intent can lose the notification after a crash; sending a notification before recording delivery can produce a duplicate when the process restarts. A durable alert-intent record with a uniqueness constraint gives the worker a stable unit of retry, and the chosen notification channel determines how far delivery deduplication can go. Exactly-once business processing remains a separate obligation.

## A small Go polling adapter with bounded retries

The following program is runnable Go using only the standard library. It performs one grouped-error query and preserves the response as JSON without guessing its fields. Set INFRAI_API_KEY from the secret store and INFRAI_BASE_URL to the service's documented HTTPS v1 base URL. The latter is configuration, not an invitation to send credentials to an arbitrary host. Run this adapter on the chosen polling schedule; inspect the verified response schema before connecting its output to the window policy.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
        parsed.User != nil || parsed.Path != "/v1" ||
        parsed.RawQuery != "" || parsed.Fragment != "" || key == "" {
        return fmt.Errorf("configure the trusted HTTPS v1 base URL and API key")
    }
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            base+"/errors/groups", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return fmt.Errorf("group query failed: %w", err)
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 4<<20 {
            return fmt.Errorf("group response exceeds local size limit")
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if seconds > 60 {
                    return fmt.Errorf("retry delay exceeds this poll's budget")
                }
                if wait := time.Duration(seconds)*time.Second; wait > delay {
                    delay = wait
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if wait := time.Until(deadline); wait > delay {
                    delay = wait
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("group query status %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("group response is not valid JSON")
        }
        _, err = os.Stdout.Write(append(body, '\n'))
        return err
    }
    return fmt.Errorf("group query retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Save the program as main.go and run it with the Go toolchain after configuring the two environment variables. Its read-only request can be retried without creating an additional business effect. It handles HTTP 429 with exponential backoff and respects a longer Retry-After delay; if that delay exceeds the execution budget, the poll fails rather than retrying early. Treat its output and error bodies as restricted diagnostic data.

The query is only the collection boundary. For a concrete application-owned policy fixture, use group checkout-balance-timeout, release moveout-r7, window 2026-11-06T10:00:00Z through 2026-11-06T10:05:00Z, and count 3. These are illustrative values, not vendor fields or captured production events. Build an incident key from group, release, and interval. Repeated observations of that key should create one durable alert intent; a count of 2 should create none.

A production worker needs durable uniqueness for that incident key, transactional creation of the alert intent, and a retrying dispatcher before this policy can be used for notification delivery. It should also validate window alignment and define how late-arriving events update a completed window. The example implements the query and its error handling; notification delivery and recurrence evaluation are separate components.

If an event appears after a window was first evaluated below threshold, a subsequent observation can still cross the threshold. Once the proposal exists, further observations of that same window should update the incident evidence without generating another initial notification. This is why a stable window key matters more than the identity of the polling process.

## Compare tools by the evidence the responder needs

The selection criterion is the kind of uncertainty each tool helps resolve. A checkout team needs recurring exception evidence, but it may also need aggregate service signals, visibility into a worker that never ran, or a richer exception investigation workflow; treating those needs as interchangeable leads to an incomplete rollback procedure.

| Option | Appropriate role in this checkout design | Boundary to evaluate |
| --- | --- | --- |
| Sentry | A dedicated error-monitoring option when exception investigation is the principal workflow. | Evaluate its issue and alerting workflow against the required grouping policy; payment reconciliation still belongs to the application. |
| Prometheus | Aggregate metrics for checkout behavior, with disciplined metric names and labels. | Metrics summarize behavior; a metric series is not the audit record of an individual payment operation. |
| Healthchecks | Monitoring expected worker activity so an absent run can become visible. | A completed heartbeat does not establish that each checkout was financially reconciled. |
| Infrai | One key and one bill across backend services reduce credential and invoice administration, while its unified REST API means no SDK is required for the Go poller to query grouped errors with a standard HTTP client. | For this design, use centralized error capture and grouped queries with an application-owned polling and notification worker. |

The last option fits a team that values a shared backend interface and is prepared to own the polling policy. Its unified API provides broad backend coverage through 295 routes across 20 modules, allowing the checkout worker and neighboring services to use a consistent interface. A team that wants its exception platform to own the incident notification workflow should assess a dedicated error-monitoring product against that requirement. Neither choice removes the need for an audit trail linking checkout attempts to financial outcomes.

Set the investigation boundary explicitly: this comparison's capture-and-polling path is for server exceptions. Browser source-map unminifying, crash symbolication, Electron minidump parsing, and session replay require a separate triage tool when those capabilities are acceptance criteria. Log correlation identifiers also should not be treated as proof that a distributed trace query or span tree is available. For a scheduled reconciliation worker, add heartbeat monitoring to detect the silent case in which no exception was captured because the task never ran.

OpenTelemetry's sampling guidance provides another relevant boundary: sampling is a telemetry design decision, whereas a reconciliation audit trail must meet the application's independent completeness requirements. Do not infer a complete payment history from sampled diagnostic evidence.

Compliance limits selection too. Keep resident data out of diagnostic payloads by design, then verify deletion, retention, export, and access requirements against the actual product contract. Where per-user log deletion or bulk export is a requirement, make that an acceptance gate before storing personal data; an exception capture endpoint and an application audit trail alone do not establish GDPR compliance. Feature flags should likewise not substitute for an independently maintained deployment approval record.

## Roll out with reconciliation as the acceptance gate

Introduce capture first, then run the poller in observation mode before connecting notifications. Use synthetic checkout operations to exercise repeated capture, duplicate observations, a poller restart, a late event, and an uncertain payment result. Verify that the release and operation evidence still leads the responder to the correct reconciliation record after an application rollback.

Promote the policy only after the durable incident key survives retries and the dispatcher has a documented duplicate-delivery policy. Keep collection functioning across the old and new application versions, record the rollback decision in the deployment audit trail, and reconcile ambiguous payment outcomes before authorizing another business operation. A successful rollback restores acceptable application behavior; financial correctness requires its own evidence.

## References

The implementation choices above distinguish error investigation, aggregate metrics, telemetry sampling, and monitoring a missing task. The primary references below describe those boundaries.

## Sources

- [Sentry documentation](https://docs.sentry.io/)
- [Prometheus metric naming and labels](https://prometheus.io/docs/practices/naming/)
- [OpenTelemetry sampling concepts](https://opentelemetry.io/docs/concepts/sampling/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
