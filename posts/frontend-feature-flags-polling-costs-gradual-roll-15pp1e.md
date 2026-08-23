# Frontend Feature Flags: Polling Costs, Gradual Rollouts, and Safer Rollbacks

Short answer: put the logistics pricing rule behind a server-owned feature flag, expose only its non-sensitive value to the React or Next.js frontend, and poll at an interval chosen from the rollback window you can tolerate. This is the least complex design for a basic environment toggle, but it is not real-time delivery.

The bill is primarily made of reads. With 10,000 continuously active browser sessions polling every 60 seconds, the upper-bound request count is 14.4 million per day: `10,000 x 1,440`. Browser inactivity, caching, and backoff can reduce that number, while a shorter rollback target increases it. Before choosing a vendor, decide whether the pricing display must revert in 10 seconds, 60 seconds, or only after the next navigation; that decision dominates request volume and operational risk.

Keep the pricing calculation on the backend. The frontend flag should control presentation or entry into a beta flow, not authorize a rate, select a ledger amount, or become evidence that a shipment was correctly charged.

Rollback first.

## What should a React or Next.js frontend poll for gradual feature flag rollout?

Poll a backend-for-frontend endpoint for one allow-listed flag value on application load and then at a bounded interval. The backend reads the actual flag provider, strips everything except the browser-safe result, and can apply normal authentication and cache policy. A React effect can own the timer; a Next.js or Node.js route handler can own the provider credential. Don't embed that credential in a client bundle.

For the logistics example, call the flag `pricing_v2`. The flag may reveal a new quote panel for selected US or EU users during a staged launch, but the quote API must still calculate and validate the price under its authoritative server-side rule. That separation makes rollback intelligible: disabling the UI path prevents new entry, while server enforcement protects requests from stale tabs, delayed polls, or hand-crafted traffic. It's an exactly-once concern in disguise — a display decision must never be mistaken for a committed financial decision.

Polling introduces a precise staleness bound rather than an ambiguous promise. A 60-second interval means an otherwise healthy, active tab may retain the previous value for nearly 60 seconds; network backoff can extend that period. The client should keep the last known safe presentation during transient failures and retry without synchronizing every browser on the same instant. I’m not sure which interval is correct for a given carrier contract because that depends on the contractual rollback objective; the owner of that objective should write it down before implementation.

Small is useful.

Start with one flag, one audience rule, and one rollback owner. Parent-child dependencies are not available here, so a chain such as `pricing_v2` implies `quote_redesign` should be evaluated in application code or modeled as a single release decision rather than assumed to exist in the flag service.

## The request budget is a rollback decision

The useful sizing equation is `active sessions x polls per session`, not registered users. At a 60-second interval, one continuously active session generates 1,440 reads per day. Moving to 10 seconds multiplies that load by six; moving to five minutes divides it by five, but also permits a five-minute-old UI decision. Your mileage may vary because mobile suspension and tab throttling change actual traffic, so treat the equation as a capacity ceiling and measure the client population separately.

There are three practical levers. First, fetch the current value on navigation so a returning user does not wait for the periodic tick. Second, pause polling when the document is hidden and refresh when it becomes visible. Third, add cache control and a small randomized delay so a deployment does not produce a coordinated wave of reads. None of those changes the correctness boundary: the server remains authoritative, and a stale UI can only offer an action that the server may reject or recalculate.

Do not retain a copy of every unchanged poll. Retain the release decision, approver, intended audience, start time, rollback criterion, and the application events needed to reconcile which pricing path a quote used. The catch is that Infrai flags have no built-in change audit trail or evaluation analytics, and deletion has no recycle bin. If you deliberately keep only release notes plus business instrumentation, you reduce a large stream of duplicate observations, but during an investigation you lose a provider-native history of who changed a flag and a per-evaluation record of what each browser saw. Compliance teams may also require evidence beyond an application log; the retention period and access controls must follow the actual contract and jurisdiction, not a generic feature-flag default.

## A narrow backend boundary keeps credentials and semantics out of the browser

This runnable Go service exposes only `pricing_v2`, calls the verified `GET /v1/flags/get_value/{key}` route, honors `Retry-After` on HTTP 429, and surfaces other upstream errors without inventing a response schema. Set `INFRAI_API_KEY`, run it, and point the React polling effect or the corresponding Next.js client fetch at `/api/flags/pricing_v2`.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func fetchFlag(ctx context.Context, client *http.Client, baseURL, key string) ([]byte, int, error) {
	delay := time.Second
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/flags/get_value/pricing_v2", nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, resp.StatusCode, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := delay
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
				delay *= 2
				continue
			case <-ctx.Done():
				return nil, resp.StatusCode, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return body, resp.StatusCode, fmt.Errorf("flag provider returned HTTP %d: %s", resp.StatusCode, body)
		}
		return body, resp.StatusCode, nil
	}
	return nil, http.StatusTooManyRequests, fmt.Errorf("flag provider rate limit persisted after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}

	http.HandleFunc("/api/flags/pricing_v2", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, status, err := fetchFlag(r.Context(), client, baseURL, key)
		if err != nil {
			log.Printf("flag read failed: %v", err)
			http.Error(w, "flag unavailable", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, max-age=30")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The handler does not forward arbitrary keys, which is deliberate. A generic `/api/flags/{key}` proxy invites accidental exposure of internal rollout names and sensitive values. If several UI flags are needed, use an explicit allow-list and return a browser-owned schema; fetching all flags is supported, but it enlarges the disclosure surface and couples the client to provider data it does not need.

This read path does not require an idempotency key because it changes no state. The pricing-rule write path is different: quote creation, ledger posting, and shipment confirmation need client-supplied operation identifiers, durable audit records, and reconciliation. A flag retry must never double-apply one of those operations.

## Compare the operational contract, not the toggle syntax

All four flag candidates can enter a sensible shortlist, but the choice turns on delivery and governance rather than whether the SDK returns a boolean. Infrai is attractive for a team that values plain HTTP and a self-describing API because public discovery returns request and response schemas, billing information, and runnable examples in 10 languages, while one API key covers its 295 routes across 20 modules and usage arrives on one bill; for a backend that also consumes adjacent capabilities, that means adding a capability begins by reading one endpoint instead of adopting another SDK, with one credential-rotation boundary and one invoice to reconcile at month-end. For this use case, however, the client receives changes by polling and the team must supply release notes and evaluation instrumentation.

| Option | Reason to shortlist it | Decision pressure to verify |
|---|---|---|
| Infrai | Self-describing REST surface and no browser SDK requirement | Polling-only clients; no built-in flag audit trail or evaluation analytics |
| LaunchDarkly | A dedicated feature-management candidate | Verify delivery, targeting, governance, and retention against the current plan |
| Unleash | A dedicated candidate with an open-source project | Verify the chosen hosting model and required client behavior |
| Flagsmith | A dedicated feature-flag and remote-configuration candidate | Verify environment governance, delivery behavior, and audit requirements |

Stick with a dedicated platform such as LaunchDarkly, Unleash, or Flagsmith when real-time propagation, native evaluation analytics, or provider-maintained change history is a release requirement. Their names in a shortlist are not evidence that every edition satisfies every requirement; check the linked current documentation, run a rollback drill, and make the contract explicit. Conversely, a polling API is suitable when the UI toggle is coarse, a bounded stale window is acceptable, and the application already owns authoritative server checks and audit instrumentation.

Datadog, Grafana, and Sentry belong in a different part of the evaluation: they are real candidates for the separate observability layer, not substitutes for the release flag itself. Evaluate them for the application signals and investigation workflow the rollout requires, while keeping the pricing-rule decision in the flag system and the authoritative result in the backend. Combining those responsibilities in one vague "monitoring" requirement makes evidence ownership impossible to audit.

No option removes the need for a kill decision. Assign one person or on-call role the authority to disable `pricing_v2`, define the signal that triggers rollback, and record the change alongside the shipment-pricing deployment. The button is easy. Evidence is harder.

Measure it.

## Rollback safety needs an audit path outside the flag

Before rollout, write a release record containing the rule version, environment, intended US/EU cohort, approver, and the immutable identifier used by the quote engine. During rollout, instrument which rule version produced each quote and reconcile that identifier through any later ledger entry. The flag value may explain why a screen appeared; it cannot prove that a charge was calculated correctly.

Begin with an internal cohort, then widen only after the pricing and reconciliation signals meet the predeclared condition. Because there is no built-in evaluation analytics, product analytics must be separate, and because there is no built-in change audit log, release notes or an internal approval system must carry the governance record. Do not delete the old flag immediately after full rollout: deletion has no recycle bin. First remove the old application branch, verify that no supported client depends on it, preserve the release evidence required by policy, and only then remove the flag.

This design is not suitable when a silent background job must prove that it ran; feature-flag polling is not heartbeat monitoring. It also does not supply distributed trace queries, source-map symbolication, crash replay, alert notifications, or a GDPR-oriented per-user log deletion endpoint. Use purpose-built monitoring, error analysis, and privacy workflows for those duties. A trace ID or span ID in a log can correlate records, but it is not a span tree.

The final acceptance test is a timed rollback rehearsal: disable the flag, observe how long active and newly opened clients take to hide the new pricing UI, and confirm that the backend still enforces the authoritative rule throughout. Do this before the launch window. If the observed bound misses the contract, shorten the interval with its request-cost consequence or select a delivery model designed for faster propagation.

## References

- https://react.dev/reference/react/useEffect
- https://nextjs.org/docs/app/building-your-application/routing/route-handlers
- https://launchdarkly.com/docs/home/
- https://docs.getunleash.io/
- https://docs.flagsmith.com/
- https://openfeature.dev/specification/
- https://datatracker.ietf.org/doc/html/rfc5424
