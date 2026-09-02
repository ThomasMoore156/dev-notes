# Edtech Fan-Out Guarantees: Node.js Queue Push Signature Verification and Ack Example

Short answer: for an edtech shipment update sent to many subscribers, accept each queue push at a public HTTPS endpoint only after signature verification and a durable idempotency claim, acknowledge that custody quickly, then fan out through an auditable outbox whose workers can repeat safely.

The least complex option that meets the delivery constraint is an at-least-once intake plus idempotent effects. It does not create literal exactly-once transport, but it gives the application an exactly-once business invariant: one shipment transition may produce many subscriber attempts, while each subscriber and event pair may commit at most one logical delivery. A Node.js deployment can implement this boundary with a raw request buffer and a database transaction; the protocol matters more than the runtime.

## How should a Node.js queue push subscriber verify a public HTTPS signature and ack?

Treat the public endpoint as a custody transfer, not as the place where every school, guardian, and campus system receives the update. The handler should read the exact request bytes under a configured size limit, verify the sender's documented signature against those bytes, validate the event envelope, and atomically insert both an inbox claim and an outbox record. Only then should it return the success status defined by the push contract. Don't parse and serialize the JSON before verification: even an equivalent object can have different bytes.

The transaction key should describe the business effect. For an update such as shipment `ship-1842` entering state `dispatched`, a useful key is the stable event ID together with the subscriber ID, rather than a process-local counter. The inbox can claim `evt-7f31` once, while the outbox creates one row per active subscriber from the same committed shipment version. This split preserves two separate facts in the audit trail: the platform accepted the source event, and a particular subscriber became eligible for delivery.

Acknowledgment has a narrow meaning. **It proves durable custody; it does not prove downstream completion.** If signature verification fails, the endpoint should reject the request under the application-owned contract, for example with `401` for absent authentication metadata or `400` for a malformed signature. If a valid duplicate reaches the endpoint after its inbox row already exists, returning the same documented success class is normally correct because custody has already been established. Slow subscriber calls do not belong on this request path.

Keep the ordering rigid:

1. Preserve the raw body and authentication metadata.
2. Verify authenticity before trusting any payload field.
3. Validate the shipment event and reject impossible state transitions.
4. Claim the source event and create fan-out work in one transaction.
5. Acknowledge according to the queue's published push contract.

Order is the defense. Two replicas can receive `evt-7f31` concurrently, both verify it successfully, and then race on the inbox insert; a database uniqueness constraint selects one transaction as the creator of fan-out work, while the other observes an already-owned event and exits without creating a second set. An in-memory set can't provide that property across a restart or across replicas.

No exceptions.

The following Go example isolates the cryptographic boundary and the durable claim interface. Its header names define an application-owned signing contract, not a claim about any queue provider. In a Node.js server, the corresponding requirements are to retain the request as a `Buffer`, compare authentication codes in constant time, and implement `Accept` with a database transaction rather than a JavaScript `Set`.

```go
package intake

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const maxEnvelopeBytes = 256 << 10

type ShipmentEvent struct {
	EventID   string `json:"event_id"`
	Shipment  string `json:"shipment_id"`
	State     string `json:"state"`
	Version   int64  `json:"version"`
}

type Acceptor interface {
	// Accept atomically inserts the inbox claim and subscriber outbox rows.
	// It returns false when this event was already accepted.
	Accept(context.Context, ShipmentEvent, [32]byte) (bool, error)
}

type Handler struct {
	Secret []byte
	Store  Acceptor
}

func authenticate(body []byte, encoded string, secret []byte) error {
	provided, err := hex.DecodeString(encoded)
	if err != nil {
		return errors.New("malformed signature")
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return errors.New("signature mismatch")
	}
	return nil
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	signature := r.Header.Get("X-Body-HMAC-SHA256")
	if signature == "" {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxEnvelopeBytes))
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := authenticate(body, signature, h.Secret); err != nil {
		http.Error(w, "invalid signature", http.StatusBadRequest)
		return
	}

	var event ShipmentEvent
	if err := json.Unmarshal(body, &event); err != nil || event.EventID == "" || event.Shipment == "" {
		http.Error(w, "invalid shipment event", http.StatusBadRequest)
		return
	}
	digest := sha256.Sum256(body)
	created, err := h.Store.Accept(r.Context(), event, digest)
	if err != nil {
		http.Error(w, "custody not established", http.StatusServiceUnavailable)
		return
	}
	if !created {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
```

There is one deliberately unresolved input: the chosen queue's exact signing headers, canonical payload rules, retry deadline, and successful response codes. Those values must come from its current delivery contract. Guessing them would turn an otherwise sound handler into a protocol mismatch.

The precise retry profile may vary.

## Delivery guarantees live in the data model

Fan-out changes the unit of correctness. The source shipment event may be unique, yet delivery state is a matrix keyed by `(event_id, subscriber_id)`, because subscriber A can accept an update while subscriber B times out and subscriber C has been disabled. Collapsing that matrix into one `delivered` Boolean loses the evidence needed for replay and reconciliation. The outbox row should therefore retain a stable effect key, attempt count, next eligible attempt time, terminal state, and a digest or reference sufficient to prove which shipment version was sent; sensitive student or guardian data should be minimized, access-controlled, and removed according to the applicable retention policy.

Exactly-once language needs discipline here. A worker can mark an outbox row complete and still lose its database response, or send an HTTPS request and lose the subscriber's reply. Consider the awkward sequence in full: the worker leases row `out-9031`, sends shipment version `12`, the subscriber commits that version, and the connection closes before the response arrives; after the lease expires, a second worker sees an apparently unfinished row and sends version `12` again. No local transaction can atomically commit both databases, and no amount of careful timing removes that uncertainty. The defensible design is at-least-once delivery with an idempotency key that the subscriber stores alongside its own state transition — the same `(event_id, subscriber_id)` key on every attempt. Where a subscriber cannot honor such a key, the sender must admit the weaker guarantee, preserve the ambiguous outcome, and reconcile it rather than silently calling it successful.

This produces a compact ledger of state changes: `eligible -> leased -> attempted -> delivered`, with `retryable` and `terminal` outcomes recorded as events rather than overwritten counters. Lease expiry permits another worker to recover abandoned work. A monotonic shipment version prevents an old `packed` notification from replacing a newer `dispatched` state after network reordering. Audit records should capture event IDs, subscriber IDs, key versions, status classes, and timestamps, but not signing secrets or full regulated payloads.

Retries require classification. Authentication failures and schema violations are permanent for that envelope; repeated delivery won't repair them. Timeouts, rate limits, and temporary connection failures are ambiguous or retryable, so use bounded exponential backoff with jitter and preserve the same effect key. Stop eventually. A dead-letter or operator-review state is preferable to an unbounded loop that obscures a broken subscriber and expands the compliance footprint.

The catch is that a push endpoint is not suitable when every receiver must remain on a private network, when consumers need independent replay over a long event history, or when a multi-step shipment process requires durable compensation and joins. In those cases, keep consumption private with a pull queue, use a replayable log, or adopt a workflow engine whose state model matches the process. A scheduled repository job is also the wrong primitive for per-event delivery; it can trigger periodic repository automation, but it does not replace subscriber-specific custody and acknowledgment.

## Compare models only after fixing the invariant

The decision axis is delivery guarantees, so the useful comparison is about ownership and recovery rather than feature count.

| Model | Custody boundary | Good fit | Material limitation |
|---|---|---|---|
| Public HTTPS push | A successful response after durable acceptance | Low-latency delivery to reachable subscribers | Public ingress, provider-specific authentication, and ambiguous downstream outcomes require careful handling |
| Private pull queue | Explicit consumer acknowledgment after a durable receive | Workers that must remain private and control concurrency | The application operates polling, leases, and worker capacity |
| FIFO queue | Ordered processing and deduplication within the documented queue semantics | Shipment transitions that require ordering within a defined group | Application-level effects still need idempotency and reconciliation |
| Scheduled trigger | Time-based invocation | Periodic sweeps for overdue or unreconciled records | Timing is not an event-delivery guarantee and fan-out state still belongs elsewhere |
| Durable workflow | Persisted orchestration state across steps | Long-running shipment flows with waits, joins, or compensation | More concepts and operational machinery than a narrow delayed notification needs |

FIFO semantics can reduce reordering inside the transport, but they do not authorize the business layer to discard version checks or effect keys; the AWS SQS FIFO documentation is useful primary evidence for the queue-specific ordering and deduplication contract, while the application must still define what a repeated shipment transition means. Likewise, scheduled workflow documentation describes when a repository workflow can be triggered, not a transactional bridge between a shipment database and thousands of subscriber endpoints.

Cost belongs in this comparison, though not as a substitute for correctness. Estimate requests from `shipment events x active subscribers x attempts`, then add storage for inbox, outbox, and audit history, outbound transfer, dead-letter review, secret rotation, dashboards, and on-call labor. A design with a low request price can still be expensive if weak retry controls multiply traffic or if reconciliation requires manual database archaeology.

## How can the ack pattern be rolled out without risking every subscriber?

Begin with a shadow path that creates inbox and outbox records but does not call subscribers, then compare its event and recipient counts with the existing shipment source. Next, enable a small subscriber cohort, inject duplicates and reordered versions, rotate a signing key, expire a worker lease, and verify that every accepted source event is either fully reconciled or visibly pending. Test deploy termination between each state transition. It should be boring.

That's the standard.

Before broadening the cohort, alert on age of the oldest eligible row, attempt distribution, terminal outcomes, duplicate claim rate, and accepted events with no outbox children. Keep rollback narrow: disable new dispatch while retaining accepted work and its audit history. The final readiness rule is concrete: no acknowledgment before durable custody, no business effect without a stable idempotency key, and no claim of delivery that cannot be reconstructed from stored state.

## Further reading

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
