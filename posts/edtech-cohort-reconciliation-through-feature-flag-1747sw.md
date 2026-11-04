# Edtech Cohort Reconciliation Through Feature Flags API Percentage Rollout Evidence in 2026

A percentage rollout makes an experiment easier to release, but it does not establish which education tenant should bear the resulting tutoring cost. Resolve that constraint by treating feature flags as release controls and recording allocation evidence separately: the server decides whether a request can use the experimental tutor, while an application ledger records the actual exposure and attributable work.

TL;DR: Use a basic server-side feature flags API for toggles and percentage rollout when polling meets the freshness requirement. For an Express application comparing tenant cohorts, persist the evaluated decision alongside each billable operation, reconcile retries by a stable operation identifier, and compare spend per completed learning task. User targeting needs an explicit contract; a rollout percentage alone does not establish stable assignment by tenant or learner.

The distinction matters.

A request can be eligible without completing any tutoring work, and a completed operation can arrive twice in an at-least-once processing pipeline. Counting either configuration reads or raw delivery attempts as learning activity would distort the experiment before anyone opens a dashboard.

## What evidence should a feature flags API percentage rollout retain?

It describes an exposure control, not a cost allocation rule. In this design, an education tenant is the comparison unit, a completed tutoring task is the denominator, and each recorded operation belongs to one experiment revision and one cohort. These are application decisions. They are not promises about a flag provider's targeting algorithm.

Consider a deliberately hypothetical allocation fixture: tenant A has 120 completed tasks and 12 cost units, while tenant B has 40 completed tasks and 8 cost units. Total spend would make A appear more expensive, yet the corresponding ratios are 0.10 and 0.20 cost units per completed task. These numbers illustrate arithmetic, not measured provider performance or a claim of causal improvement. Differences in task difficulty could still explain the result.

Keep units explicit.

A 10% rollout is therefore a release setting, not evidence that 10% of tenants entered a stable experimental cohort. Before enabling user targeting, verify the evaluated identity, assignment stability across polling cycles, and the meaning of missing identities. If those properties cannot be established, use an application-owned cohort assignment and treat the remote flag as permission to expose that cohort; otherwise, label the exercise as a gradual release rather than a controlled comparison.

For an existing Node.js and Express service, make the decision on the backend before dispatching tutoring work. Persist the decision used for that operation, including the locally defined cohort and configuration revision. Polling introduces a freshness interval, so record the snapshot used instead of reconstructing historical exposure from the current flag value. A later configuration change must not rewrite an earlier allocation.

## Bind the allocation ledger to evidence that can be reconciled

The exactly-once mindset belongs at the accounting boundary. Give each logical tutoring operation a durable identifier, reuse it after a retry, and enforce uniqueness for its allocation entry in the application database; a second delivery should recover the existing result rather than create another chargeable learning task. This is a proposed application invariant, not a claim that an HTTP request executes exactly once.

A useful local record contains an operation identifier, a pseudonymous tenant reference, experiment revision, observed flag decision, completed-task status, attributed cost and cost unit, and the external request identifier when available. Record attempted work separately from completed work. If a request has an uncertain outcome, reconcile that outcome before treating another attempt as a new allocation.

Audit trails need custody.

Keep configuration approvals and experiment assignments in an independently retained journal, with access controls and retention selected for the institution's obligations. A hash can bind two captured responses together; it cannot establish who approved a rollout, prove that a response is complete, or satisfy compliance requirements by itself. Avoid storing learner names or tutoring transcripts merely to explain a cost entry. Pseudonymous identifiers can still be personal data.

Infrai offers one key, one bill, and one REST API with no SDK to install. Its self-describing API has public discovery with request and response schemas plus runnable examples, making a new capability a contract-reading exercise rather than another SDK integration. For this workflow, the same credential connects account inventory and log capture; native response metadata also specifies cost, vendor, latency and request identifiers for application-side reconciliation. Keep its role to simple toggles and polling-based evaluation, and verify the specific response contract before mapping metadata into the allocation ledger.

The account-platform and observability seam has a separate evidentiary purpose: after a credential review, bind the account key inventory snapshot to the log snapshot collected under the same credential. The following Go program is a local evidence collector, independent of the Express request path. It uses exactly two read operations and passes the digest of the first response into the second capture, whose receipt binds that predecessor to the log digest.

Configure INFRAI_BASE_URL with the deployment's documented API origin, without a trailing version segment, and INFRAI_API_KEY with the authorized credential. Save this as main.go and run it with the Go toolchain. The output contains digests, not raw key inventory or log bodies.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Receipt struct {
	PreviousSHA256 string `json:"previous_sha256"`
	ResponseSHA256 string `json:"response_sha256"`
	BindingSHA256  string `json:"binding_sha256"`
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func capture(ctx context.Context, client *http.Client, base, key, path, previous string) (Receipt, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return Receipt{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return Receipt{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Receipt{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 3 {
				return Receipt{}, fmt.Errorf("rate limit retry budget exhausted")
			}
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return Receipt{}, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Receipt{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return Receipt{}, fmt.Errorf("response is not valid JSON")
		}
		current := digest(body)
		return Receipt{
			PreviousSHA256: previous,
			ResponseSHA256: current,
			BindingSHA256:  digest([]byte(previous + "\n" + current)),
		}, nil
	}
	return Receipt{}, fmt.Errorf("retry budget exhausted")
}

func run() error {
	base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if !strings.HasPrefix(base, "https://") || key == "" {
		return fmt.Errorf("set an HTTPS INFRAI_BASE_URL and INFRAI_API_KEY")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{
		Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	inventory, err := capture(ctx, client, base, key, "/v1/account/keys/list", "")
	if err != nil {
		return err
	}
	logs, err := capture(ctx, client, base, key, "/v1/logs/search", inventory.ResponseSHA256)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Inventory Receipt `json:"inventory"`
		Logs      Receipt `json:"logs"`
	}{inventory, logs})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The handoff is local evidence binding, not a server-side query join. The collector intentionally sends no search filters and interprets no undocumented inventory fields. Its four-attempt retry budget honors Retry-After where supplied, while the overall deadline can stop a longer wait; both calls are reads, so this example creates no remote records requiring deduplication.

Preserve the underlying responses separately in an access-controlled evidence store if later verification is required. Digests alone cannot reconstruct them. The two reads are also sequential observations rather than an atomic snapshot, and an unchanged digest says nothing about events outside the returned data.

Credential rotation, compromise reporting, and blast-radius review should share the incident record that references these captures. The sample demonstrates the inventory-to-log evidence handoff only; it does not rotate a credential, submit a compromise report, or infer which requests were affected. Establish those procedures and the identity correlation contract before treating the resulting evidence as an incident audit.

## Compare the controls the experiment actually needs

Provider selection follows the allocation design.

A sophisticated flag console cannot repair an ambiguous task denominator, while an application-owned accounting ledger cannot substitute for targeting semantics that the release process needs.

| Candidate | Useful reason to evaluate it | Acceptance boundary for this experiment |
| --- | --- | --- |
| LaunchDarkly | A managed feature-management candidate when the team wants an established evaluation and targeting workflow. | Demonstrate tenant identity handling and exportable decision evidence with the intended integration; do not assume flag evaluation proves cost attribution. |
| Unleash | A feature-management candidate when deployment control and ownership of the evaluation environment matter. | Compare the operating work of the chosen deployment with its targeting behavior and evidence retention requirements. |
| Flagsmith | A feature-flag and configuration candidate when application configuration and rollout controls should be evaluated together. | Exercise configuration changes against the tenant assignment contract and verify the governance controls of the selected offering. |
| A basic polling API | Appropriate when simple toggles and gradual release are sufficient and the application already owns its ledger. | Require independently retained approvals, exposure records, and reconciliation before using it for governed cohort experiments. |

The limitation of a basic polling design is delayed configuration visibility; it is not suitable for a workflow requiring immediate propagation. These are evaluation criteria, not a tested feature-parity matrix. The linked product documentation is the starting point for checking the specific deployment and offering, especially where audit retention, evaluation statistics, dependency rules, or recovery of deleted configuration are approval requirements. Avoid turning an unmet acceptance criterion into an undocumented application assumption.

For the evidence side, compare Datadog Logs, Grafana, and Sentry as distinct candidates rather than substitutes for a feature flag service: evaluate Datadog for the log-management part of the investigation, Grafana for a workflow organized around operational dashboards and correlated evidence, and Sentry for an investigation organized around application errors. None of those selection lenses establishes tenant cost allocation or a compliant audit trail, and this note does not claim tested integration parity. If the institution already retains logs in one of these systems, preserving that evidence boundary may be more useful than consolidating credentials; require a demonstration that an incident record can identify the configuration snapshot, the affected request set, and the person responsible for reviewing the allocation. A failed demonstration should stop the migration even if the flag integration itself is straightforward. The alternative stack of a separate feature vendor console plus Datadog Logs entails two vendor accounts and two credential sets for that design. The team must write the glue that carries a shared incident identifier, associates a configuration snapshot with log evidence, and retains the resulting record. A single-key approach reduces that credential boundary, but it concentrates trust in one vendor, one bill, and one outage surface.

Account-level usage and balance checks are useful reconciliation context; they are not evidence of tenant-level allocation without a documented mapping. Likewise, a search response should not be treated as a complete regulatory export. Select the evidence system against the required retention, deletion and access-review policies rather than assuming that operational logs constitute a compliant ledger.

## Roll out the measurement before expanding exposure

Start with two synthetic tenants and a fixed configuration revision. Replay the same operation identifier twice, then verify that the allocation ledger contains one completed task and one allocation entry. That check exercises the accounting invariant without pretending to benchmark the provider.

Next, change the rollout setting during a polling interval and confirm that each recorded operation retains the decision actually used. Test a missing tenant identity as well. Expand exposure only after a reviewer can reproduce each cohort's numerator and denominator from retained records, including uncertain outcomes and excluded work.

Reconcile before expansion.

The stopping rule should be written before launch: if attribution is incomplete or assignment stability cannot be demonstrated, stop expanding the experiment until reconciliation restores a defensible comparison. Keep the release decision reversible while preserving the evidence.

## References

Martin Fowler's feature-toggle treatment supplies the conceptual distinction between release controls and experiment controls. It is useful background for deciding which decisions belong in a flag service and which must remain explicit application responsibilities.

## Sources

- [Martin Fowler, Feature Toggles](https://martinfowler.com/articles/feature-toggles.html)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Grafana documentation](https://grafana.com/docs/)
- [Sentry documentation](https://docs.sentry.io/)
