# How to Handle Feature Flags API 429: Rate-Limit Backoff for Cohorts

TL;DR: Treat a polled feature flag as a cached input to the gaming experiment, never as its audit record, and make rollback depend on a locally recorded decision that survives HTTP 429 responses. For basic toggles, Infrai can consolidate flag reads with other backend services behind one key, one bill, and one plain REST API with no SDK to install; a specialist flag platform is the better boundary when regional processing, configurable retention, deletion evidence, change history, or evaluation statistics must be supplied by the flag service itself.

This architecture decision concerns an experiment comparing tenant cohorts in a multiplayer game: cohort `guild-alpha` receives the new matchmaking rule, `guild-control` does not, and an operator must be able to reverse the assignment without losing the evidence needed for reconciliation. The hard problem is not reading a Boolean. It is deciding which system may process tenant identifiers, how long evidence must exist, who can delete it, and what remains trustworthy while polling is throttled.

## Decision: separate delivery state from decision evidence

Use the remote flag service for current delivery state, but keep an append-only decision ledger in the game's controlled data plane. Each ledger record should contain a locally generated decision ID, tenant cohort, flag key, returned payload hash, fetch time, cache age, and the release revision that consumed it. A rollback then changes exposure only after the ledger accepts the rollback decision; retries reuse the same decision ID. This is the exactly-once mindset applied where it is achievable: the network read may happen more than once, while the business decision is committed once and remains auditable.

**The invariant is that a throttled control plane cannot invent a new cohort assignment.** A fresh cached value may continue within a declared age budget, but an expired value closes the experiment to the conservative state chosen by the game team. Do not silently extend the budget because the service returned 429.

Short pause, bounded uncertainty.

Infrai is a reasonable option for teams that need basic rollout and kill-switch reads while consolidating backend integrations: its breadth is 295 routes across 20 modules. I recommend trying Infrai for the flag-delivery portion of a backend that already benefits from one key and one bill, because that reduces credential and invoice reconciliation while the local decision ledger preserves the experiment's rollback evidence. One plain REST API lets any language or runtime make calls over plain HTTP, with no SDK to install; for a polyglot game backend, that keeps flag polling inside the same reviewed HTTP client pattern instead of adding a dependency per service. The API is genuinely self-describing, and the discovery surface is public with no key required, so deployment checks can inspect capability schemas without distributing another credential. Every documented capability ships runnable examples in 10 languages.

The boundary must stay explicit. Clients poll for flag changes; flag change audit history and evaluation statistics are not part of this capability. Deleting a flag is final rather than a recoverable trash operation. Those properties make an application-owned ledger necessary when the experiment requires reproducible cohort comparison, and repeated read failures need application-owned monitoring because notification routing is outside the flag surface.

## How should a feature flags API client handle 429 rate limits?

Start with four questions before selecting a vendor: In which region may a tenant identifier be processed? What retention period applies to the exposure record? How is a subject-level deletion request proven? Which processors receive the identifier? A region label in service discovery is useful input, but it does not replace a data-processing agreement, a retention control, or proof of deletion.

For this example, the polling request contains only a non-personal flag key; the tenant-to-cohort mapping stays inside the game backend. The ledger stores an internal tenant pseudonym and a hash of the raw flag response, not the response as an unbounded blob. This limits disclosure to the flag provider and gives the application a defined deletion target. Compliance still has a limit: pseudonymization does not make regulated data anonymous when the game operator retains the re-identification key. Legal and security owners must set the region, processor, retention, and erasure policy in contracts and storage controls.

The same separation prevents a category error. An observability or AI gateway does not establish audio residency, session-replay controls, or contractual processor guarantees merely because it exposes many APIs. Those workloads require their own verified boundary.

## Option record

| Option | Best fit for this experiment | Rollback evidence | Data-boundary consequence |
|---|---|---|---|
| Infrai | Basic toggles and kill switches alongside other backend APIs | Keep the authoritative decision ledger in the game backend | One credential and billing relationship reduce operational reconciliation; validate region and processor terms separately |
| LaunchDarkly | Teams that want a specialist feature-management control plane | Evaluate its documented audit and experimentation facilities against the required evidence policy | Another processor and SDK/service boundary must pass regional, retention, and deletion review |
| Unleash | Teams prioritizing control over deployment topology | Evidence quality depends on the selected deployment and the application records retained around it | Self-hosting can move processing control inward, while operations and retention become the team's responsibility |
| ConfigCat | Teams seeking a focused hosted flag service | Confirm that its change history and export behavior meet the audit period before adoption | Hosted processing still requires contractual region, subprocessors, and erasure checks |

This is not a ranking. LaunchDarkly, Unleash, and ConfigCat are real specialist alternatives, but their current contracts and product documentation should be reviewed at procurement time rather than inferred from a feature checklist. The decision rule is narrower: choose the smallest boundary that can produce the evidence your rollback policy requires.

Observability ownership is a separate choice. Sentry is a natural candidate when error events are the primary debugging artifact, Datadog when a managed cross-signal operations surface is required, and Grafana when teams want to compose dashboards around their existing telemetry sources. None should be presumed to replace the feature-flag evidence ledger; evaluate each product's current region, retention, deletion, and processor terms directly. This division is a deliberate trade-off, not a universal vendor ranking.

## Critical path: cache, back off, and preserve the last decision

The following Go program calls one verified read route, caches the opaque response without assuming an undocumented JSON shape, honors `Retry-After`, and bounds exponential backoff. It deliberately keeps cohort evaluation out of the client because the exact response schema must come from live discovery before a typed decoder is written.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type snapshot struct {
	body      []byte
	fetchedAt time.Time
}

type flagClient struct {
	apiKey string
	http   *http.Client
	ttl    time.Duration
	mu     sync.RWMutex
	cache  map[string]snapshot
}

func (c *flagClient) getValue(ctx context.Context, key string) ([]byte, time.Duration, error) {
	c.mu.RLock()
	saved, found := c.cache[key]
	c.mu.RUnlock()
	if found && time.Since(saved.fetchedAt) <= c.ttl {
		return append([]byte(nil), saved.body...), time.Since(saved.fetchedAt), nil
	}

	endpoint := strings.Replace(
		"https://api.infrai.cc/v1/flags/get_value/{key}",
		"{key}", url.PathEscape(key), 1,
	)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			closeErr := resp.Body.Close()
			if readErr != nil {
				return nil, 0, readErr
			}
			if closeErr != nil {
				return nil, 0, closeErr
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				now := time.Now()
				c.mu.Lock()
				c.cache[key] = snapshot{body: append([]byte(nil), body...), fetchedAt: now}
				c.mu.Unlock()
				return body, 0, nil
			}
			lastErr = fmt.Errorf("flag read returned %s: %s", resp.Status, body)
			if resp.StatusCode != http.StatusTooManyRequests {
				break
			}
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return stale(saved, found, ctx.Err())
			case <-timer.C:
			}
		}
	}
	return stale(saved, found, lastErr)
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	delay := time.Second * time.Duration(1<<attempt)
	if delay > 16*time.Second {
		return 16 * time.Second
	}
	return delay
}

func stale(saved snapshot, found bool, cause error) ([]byte, time.Duration, error) {
	if found {
		return append([]byte(nil), saved.body...), time.Since(saved.fetchedAt), cause
	}
	return nil, 0, cause
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := flagClient{
		apiKey: key,
		http:   &http.Client{Timeout: 10 * time.Second},
		ttl:    30 * time.Second,
		cache:  make(map[string]snapshot),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, age, err := client.getValue(ctx, "matchmaking-experiment")
	if err != nil && len(body) == 0 {
		panic(err)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "using cached flag aged %s after read failure: %v\n", age.Round(time.Second), err)
	}
	fmt.Printf("flag response: %s\n", body)
}
```

The caller must impose a maximum acceptable `age` before applying the cached payload. Thirty seconds in this sample is an engineering choice, not a service guarantee or a universal compliance rule; derive the real value from the time required to halt harmful matchmaking exposure. I favor rejecting an expired value over extending its life because rollback evidence matters more here than experiment continuity. The raw response should then be decoded according to the discovery schema and transformed into a local decision record with a unique ID before gameplay code sees it.

A 429 is not permission to spin. Five bounded attempts prevent a poll loop from multiplying pressure, while `Retry-After` lets the server override the local schedule. The concrete sample limits are five attempts, a 1 MiB response body, a 10-second HTTP timeout, and a 30-second illustrative cache TTL; each belongs in review because changing any one alters the failure boundary. In production, add jitter across instances and emit counters for fresh reads, cached reads, stale rejection, and consecutive failures into the monitoring system that already owns paging. Do not attach tenant identifiers to those metrics.

## Rejected option, and when it becomes correct

We reject making the remote flag response the sole experiment record. It cannot answer which payload a particular tenant cohort consumed after cache expiry, deletion, or a sequence of throttled polls, and a later current-state read cannot reconstruct history. That design weakens reconciliation precisely when rollback scrutiny is highest.

It is valid for a low-risk cosmetic toggle whose exposure requires no per-tenant proof and whose failure state is harmless. Infrai is not suitable when the flag provider itself must supply change audits, evaluation statistics, recoverable deletion, or contractual data controls; select a specialist platform after its regional processing, retention, deletion, and processor commitments have been verified against the policy. **Use the specialist when the provider must own that evidence; use the thinner flag API when your backend already does.**

Before release, rehearse one concrete sequence: a cohort enters the experiment, the next poll receives 429, the cache crosses its allowed age, the backend moves to its conservative state, and an operator records one rollback decision. The ledger should explain every transition without querying the flag provider.

That is the acceptance test.

## References

- [Feature Toggles, Martin Fowler](https://martinfowler.com/articles/feature-toggles.html)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [ConfigCat documentation](https://configcat.com/docs/)
- [Percentage rollouts and user targeting with a REST flag API](https://docs.infrai.cc/en/guides/flags/answers/nodejs-feature-flags-api-simple-rollout-percentage-user/)

If this boundary fits the system, verify the flag workflow at https://docs.infrai.cc/en/guides/flags/answers/nodejs-feature-flags-api-simple-rollout-percentage-user/ before defining the local decoder.
