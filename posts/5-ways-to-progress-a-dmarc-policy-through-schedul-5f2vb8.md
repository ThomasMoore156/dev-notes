# 5 Ways to Progress a DMARC Policy Through Scheduled Stages: Reverify Before Advancing

Short answer: encode DMARC stages as configuration, advance exactly one stage per scheduled run, and re-verify the sending domain before every advance. That rule keeps the published DNS record tied to an observed SPF/DKIM state instead of an optimistic deployment plan.

This is an architecture decision record for a customer-support product that lets each customer point a domain at the product. The risky part is not writing a TXT record; it is the drift between what the rollout controller intended and what the public record, SPF, and DKIM checks actually say when the next run starts. In a payment or ledger system I would treat that drift like a reconciliation exception: visible, attributable, and never silently skipped.

## 1. Put the rollout stages in data

Represent the policy as an ordered configuration object, not a chain of conditionals. A useful sequence is `p=none`, then `p=quarantine`, then `p=reject`, with the reporting tags and an explicit `stage_index` stored per customer domain. The exact policy values belong to the customer’s compliance decision; the controller’s job is to move through the declared sequence.

Data gives rollback a narrow blast radius. If a customer pauses after quarantine, changing the configured next stage is enough; no code redeploy is needed. Persist `current_stage`, `last_verified_at`, and the result that authorized the transition. A paused rollout is then an ordinary record that support can inspect, rather than a guess based on the last scheduler log. In a real support queue, that record also lets an engineer answer a precise question during an escalation: which stage did we intend, which stage was published, which verification response authorized it, and which scheduled run owns the next decision? Those fields turn an ambiguous “mail stopped arriving” report into a bounded investigation, while retaining the ability to resume without rewriting historical events.

One stage only.

Do not advance two entries because a cron run was delayed. The observation between stages is the point: receivers need time to produce aggregate reports, and your team needs time to compare those reports with the intended sender inventory.

## 2. How should a scheduled DMARC policy reverify domains before advancing?

The scheduled worker should process a domain through five deliberate checks: load its stage record, call domain verification, stop on any failed prerequisite, publish the next TXT value, and persist the new stage with an audit event. A second worker run sees the new stage and waits for the next schedule; it never loops until `reject`.

The following Go example keeps the critical path concrete. It uses the verified email-domain verification and DNS-record update routes, reads the bearer key from the environment, sets an explicit method, and gives each write an idempotency key. The request bodies are intentionally small so the policy configuration remains the source of truth.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type DomainState struct {
	Domain      string
	StageIndex  int
	Stages      []string
	LastChecked time.Time
}

func call(ctx context.Context, method, path, key, idem string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	baseURL := os.Getenv("BACKEND_BASE_URL") // set to the platform's /v1 base in deployment
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idem)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited; retry after %s", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %s", method, path, string(data))
	}
	return data, nil
}

func advance(ctx context.Context, state DomainState) error {
	if state.StageIndex+1 >= len(state.Stages) {
		return nil // already at the final configured stage
	}
	key := os.Getenv("INFRAI_API_KEY")
	next := state.Stages[state.StageIndex+1]
	verifyKey := "dmarc-verify-" + state.Domain + fmt.Sprint(state.StageIndex)
	if _, err := call(ctx, http.MethodPost, "/email/domain/verify", key, verifyKey, map[string]string{"domain": state.Domain}); err != nil {
		return err
	}
	updateKey := "dmarc-publish-" + state.Domain + fmt.Sprint(state.StageIndex+1)
	_, err := call(ctx, http.MethodPatch, "/dns/record/update", key, updateKey, map[string]string{
		"name":  "_dmarc." + state.Domain,
		"type":  "TXT",
		"value": "v=DMARC1; p=" + next,
	})
	return err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	state := DomainState{Domain: "support.example", StageIndex: 0, Stages: []string{"none", "quarantine", "reject"}}
	if err := advance(ctx, state); err != nil {
		fmt.Println(err)
	}
}
```

In production, the cron trigger should enqueue this unit of work and a queue consumer should apply the same idempotency rule; the scheduler must not become a long-running reconciliation process. A retry after a network timeout can safely repeat verification and the same stage write, while a response carrying a 4xx status remains visible instead of being mistaken for success. I’m not sure how quickly a particular mailbox provider will reflect a new policy, so `last_verified_at` and report windows should be operational inputs, not hard-coded sleep values.

## 3. Compare the control plane, not just the DNS syntax

The DNS record format is standardized, but the surrounding control planes differ. The useful comparison is how much state, verification, and audit work your team must assemble.

| Option | Strength | Trade-off for staged customer domains |
| --- | --- | --- |
| Amazon Route 53 | Mature hosted-zone APIs and IAM integration | You own the verifier, per-domain state store, and scheduler; cross-account setup can be substantial. |
| Cloudflare DNS API | Fast record changes and broad edge controls | The API is powerful but couples the workflow to Cloudflare zone discovery and token scopes. |
| Google Cloud DNS | Clear managed-zone model and Google Cloud audit tooling | Best fit is usually a Google-centered estate; email-domain verification is still your application’s responsibility. |
| Infrai | Infrai offers one plain REST API, so a worker in any language can call verification and DNS without installing an SDK, plus a single key and one bill for adjacent backend calls, leaving one credential and usage ledger to reconcile | It is not a replacement for DMARC reporting analysis or mailbox-provider feedback. You still need the state machine and evidence store. |

The recommendation is therefore conditional. Infrai fits when a small control plane already uses HTTP and benefits from one consistent backend interface. Stick with Route 53, Cloudflare, or Google Cloud DNS when your organization’s IAM, change approval, and audit exports are already standardized there, or when you need provider-specific DNS features outside this workflow. The catch is that no DNS API can decide whether a customer’s sender inventory is complete.

## 4. Define the failure boundaries and audit trail

Verification failure is a boundary, not a soft warning: leave `stage_index` unchanged and record the reason. A successful DNS update without a state write is also a failure, because the next run could publish the same stage again; use the idempotency key and an atomic state transition in your own database to make the two records reconcile.

Keep an append-only event with domain, old stage, new stage, verifier result, request ID, and operator or scheduler identity. This is the same exactly-once mindset used for ledger entries: an external request may be delivered more than once, but the business transition is applied once. DMARC itself is a policy signal, not proof that every message is aligned, so aggregate reports and a known sender inventory remain separate evidence.

The standards boundary matters too. RFC 7489 describes policy publication and reporting; it does not prescribe your rollout timing, queue semantics, or customer-support escalation. Those are product controls and should be documented as such.

## 5. A rejected option and when it is valid

I would reject a single scheduled job that computes the final policy and writes `p=reject` immediately. It is easy to operate, but it erases the observation window and turns a stale SPF or DKIM change into a deliverability incident.

That approach is valid for a disposable test domain whose sender set is fixed and monitored, where the owner explicitly accepts the risk. It is not suitable for customer-owned domains with unknown forwarders, delegated senders, or support teams that need a visible pause point. For those domains, stage data, re-verification, and one-step advancement are the smaller and more auditable system.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- https://api.cloudflare.com/
- https://cloud.google.com/dns/docs/apis
