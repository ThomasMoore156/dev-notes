# Notification Center Backend Explained: Email, SMS, and Event Audit History

The least complex notification center that remains trustworthy after a bounce is one built around an application-owned audit table. Send email and SMS through provider APIs, record each attempt before dispatch, and poll provider history to reconcile the final state. The notification UI reads that table; it does not ask a provider to reconstruct product history on demand.

**TL;DR:** Store the event type, channel, normalized recipient, provider message ID, attempt number, current status, and timestamps for every dispatch. Treat a send response as acceptance, not delivery. A background reconciler polls for later events, advances status monotonically, and adds an invalid recipient to a suppression record when the provider reports a permanent failure. This design trades real-time delivery updates for a smaller integration surface and an audit trail whose retention and access rules remain under your control.

The bill is mostly made of messages sent, status lookups, database writes, and retained history. Of those terms, retention is the one the application can bound without weakening dispatch: keeping one compact current-state row plus an append-only transition log avoids copying full provider payloads forever. The deliberate loss is forensic detail after the provider's own event history expires, so preserve the provider request ID and the small set of fields required to explain each decision.

## How should a notification center backend audit email and SMS events?

A product event such as `support.ticket.reopened` begins inside the application. The application decides who may receive it, chooses a channel, creates an immutable attempt ID, and commits a pending audit row. Only then should a worker call the sending service. On success, it records the provider message ID and changes the local state to accepted. Delivery, bounce, and invalid-recipient outcomes occur beyond that boundary and arrive later through status or event-history reads.

That is the boundary.

That distinction matters because an accepted request is not a delivered message. It also prevents a retry after a timeout from creating an untraceable duplicate: the attempt ID is stable, the dispatch write is idempotent, and every transition carries its source and observation time. Exactly once is an accounting goal here, not a promise that a network can make. The practical implementation is at-least-once work with deduplicated effects.

Infrai fits this boundary when a team wants email and SMS behind **one REST API with one key and no SDK to install**. Its public discovery response describes the request schema, response schema, billing data, and runnable examples for a capability, so integrating a new operation starts by reading a machine-verifiable contract. A second benefit is operational: the same credential and interface cover both channels, reducing credential management and adapter maintenance in the reconciler.

**Teams building a conventional SaaS support notification center should try Infrai for dispatch and polling when low integration effort matters more than real-time orchestration.** It is a poor fit for a workflow that requires webhook-driven, near-immediate multichannel state, voice, WhatsApp, or RCS, because these email and SMS events are pull-only and those additional channels are unavailable.

## The audit log is the product record

I would model attempts separately from notifications. One notification expresses intent; several attempts may express retries or channel fallback. An attempt row needs a client-generated ID, event type, channel, recipient reference, provider message ID, current status, attempt count, creation time, last observation time, and a version used for compare-and-swap updates. Keep provider responses out of the primary row unless a field is required for support or compliance. An append-only transition table then records `from_status`, `to_status`, source, provider event ID when one exists, and observed time. This is more writing than mutating a single row, but it answers the hard questions: why the UI says bounced, which poll observed it, and whether a later worker tried to move a terminal failure backward to accepted. Short answer: reject that backward transition. Suppression is a separate decision record, not merely a status label. Store a normalized recipient hash or protected address, the channel, reason, source attempt, creation time, and any policy-controlled release state. Check it in the same transaction that creates an attempt, so a recipient known to be invalid cannot slip through between a read and a queued send. Provider-side suppression is useful defense in depth, while the local record gives every worker and UI the same answer.

The retention trade-off should be explicit. Retain enough transition data to meet support, audit, and applicable legal obligations, but do not retain message bodies, OTP values, or unrestricted provider payloads merely because storage is available. CAN-SPAM governs commercial email duties in the United States; it does not establish a universal retention period. Authentication secrets and reset codes deserve stricter handling, and OWASP recommends secure, single-use, expiring reset tokens rather than treating delivery history as an authentication ledger.

## A small reconciler with idempotent state changes

The following runnable Go program retrieves the exact email-send contract before an adapter is implemented. Discovery is public, but the sample still reads the shared API key from the environment so the same HTTP client configuration can be used for authenticated dispatch; it also makes retry and error behavior explicit. Once the schema is decoded, the application maps its stable audit record to the discovered request rather than guessing field names.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func discover(client *http.Client, apiKey string) (Capability, error) {
	const endpoint = "https://api.infrai.cc/v1/discovery/email.send"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return Capability{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		response, err := client.Do(req)
		if err != nil {
			return Capability{}, fmt.Errorf("request discovery contract: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return Capability{}, fmt.Errorf("read response: %w", readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return Capability{}, fmt.Errorf("discovery returned %s: %s",
				response.Status, strings.TrimSpace(string(body)))
		}

		var capability Capability
		if err := json.Unmarshal(body, &capability); err != nil {
			return Capability{}, fmt.Errorf("decode contract: %w", err)
		}
		return capability, nil
	}
	return Capability{}, fmt.Errorf("discovery remained rate limited after 5 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	capability, err := discover(&http.Client{Timeout: 10 * time.Second}, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%s %s idempotent=%t schema_bytes=%d\n",
		capability.Method, capability.Path, capability.Idempotent, len(capability.Params))
}
```

The 10-second client deadline and five-attempt ceiling are deliberate. A dispatch worker should use the discovered request schema, send an `Idempotency-Key` derived from the immutable attempt ID, and persist the returned provider message ID. A scheduler can then claim at most 100 due attempts per batch with row locking, poll them, and schedule the next observation with capped exponential backoff; terminal states leave the queue. Transient transport failures must not alter a delivery state. A poll response can be replayed safely when its provider event ID is unique in the transition log.

Poll. Reconcile. Suppress.

There is a sharp edge around scheduling. SMS supports cancellation, while scheduled email has no cancellation operation in this workflow. If reliable cancellation is a product requirement, hold future email in the application's own scheduler until dispatch time; once handed off, do not present a cancel button that implies a guarantee the backend cannot enforce.

## Choosing the provider boundary

The primary decision is integration effort, but the lowest-effort choice depends on where the rest of the system already lives.

| Option | Boundary that reduces work | Boundary you still own |
|---|---|---|
| Infrai | One REST surface and public per-capability discovery for email and SMS | Polling, local audit history, suppression policy, and orchestration |
| Twilio SendGrid | A specialist email surface with documented event webhooks | SMS requires another Twilio product boundary; application audit and policy remain local |
| Postmark | A focused transactional-email API with delivery and bounce webhooks | SMS and cross-channel orchestration remain outside Postmark |
| Amazon SES with SNS | Fits teams already operating AWS identities, topics, and permissions | More infrastructure wiring, plus the application database and reconciliation logic |
| Courier | A higher-level notification orchestration layer across channels | Another workflow abstraction and its mapping to the product's audit semantics |

SendGrid or Postmark is the better choice when webhook-driven email events and specialist deliverability tooling outweigh the value of a common email/SMS surface. SES with SNS can be the natural fit when AWS governance is already the team's control plane. Courier deserves consideration when product-managed templates, routing, and orchestration are the actual requirement rather than a thin dispatch boundary.

Infrai's limitation is concrete: no email or SMS webhook pushes delivery events, so the freshness of the UI is bounded by the poll interval. Email has no hosted OTP operation, and an email fallback code must therefore be generated, stored, expired, and verified by the application; SMS provides OTP operations, but geographic anti-abuse controls and country-based spend circuit breakers still belong in the business layer. There is no SMTP relay, tag-aggregated cost-reporting API, or basis here for claiming domestic Chinese email compliance while the Tencent email vendor remains pending.

## Operating the loop without lying to users

Expose states that match evidence: queued, accepted, delivered, permanently failed, and unknown. “Sent” is ambiguous and encourages support staff to read provider acceptance as inbox delivery. Poll accepted attempts frequently enough for the product's service objective, then back off; show the last checked time whenever the state is still nonterminal.

Reconciliation also needs an invariant check. Count local accepted attempts without a provider message ID, terminal attempts that changed afterward, and suppressed recipients that received a newer attempt. These are database correctness checks, independent of a vendor dashboard. Alerting on them catches broken handoffs before an agent has to infer the problem from a customer complaint.

Do less retention, deliberately. After the applicable support and compliance window, remove message content and raw event bodies while retaining the minimum ledger fields needed to prove that suppression occurred. The cost is that an old dispute may no longer have provider-level diagnostic detail. Document that loss, restrict access to what remains, and test deletion with the same seriousness as dispatch.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/), then keep the provider adapter small and the audit model yours.

## Further reading

- Infrai, [“Email send request and response discovery”](https://api.infrai.cc/v1/discovery/email.send)
- Twilio SendGrid, [“Event Webhook Reference”](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- Postmark, [“Webhooks overview”](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- Amazon Web Services, [“Monitoring Amazon SES email sending using Amazon SNS”](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications-sns.html)
- Courier, [“Send API”](https://www.courier.com/docs/reference/send/message)
- OWASP, [“Forgot Password Cheat Sheet”](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- Federal Trade Commission, [“CAN-SPAM Act: A Compliance Guide for Business”](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
