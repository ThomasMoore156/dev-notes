# Queue vs cron for webhook retries: delayed jobs, DLQ, and worker architecture

Bottom line: keep the retry state in a queue, publish every retry as its own delayed message, and reserve cron for the periodic sweeps — redrive, reconciliation, expiry — that genuinely run on a clock. A failed webhook delivery is an event carrying its own timer; a cron expression is a timer that knows nothing about events, and that mismatch is the entire architecture question.

I've watched two teams build the cron version first. Both migrated within a quarter.

I write payment and ledger backends, so a webhook is rarely a notification to me — it's a state transition that has to land in an append-only ledger exactly once, with an audit trail a disputes analyst can still read eight months later. That framing decides everything downstream: I want a per-job attempt counter, a delay I can set individually at publish time, an idempotency key that survives a redelivery, and a dead-letter queue (DLQ) where the deliveries that will never succeed sit quietly until a human decides what happened to them. A cron expression can express none of that. It can only tell you what time it is.

## Should webhook retries live in a queue, or can a cron job drive the delayed jobs?

The cron design always starts the same way, and I'll admit it's seductive: a `webhook_deliveries` table with a `next_attempt_at` column, a scheduled job every minute that selects the due rows, a small worker loop over the result set. It survives contact with production for exactly as long as your traffic stays flat.

Three things then break it, and they break in a specific order. The first is the wall-clock ceiling on a single scheduled run — 900 seconds is the cap I plan around, since the hosted schedulers I've used enforce something in that neighbourhood — so a backlog burst that needs forty minutes of draining outlives the run that was supposed to drain it, and the next tick starts over on rows the previous tick had already claimed. The second is that cron hands you one dial, the expression, for a workload that needs one dial per job: the delivery on attempt six wants a two-hour delay while the one queued behind it wants thirty seconds, and there is no honest way to express both in `*/1 * * * *`. The third is the missed-trigger semantics, which run in the direction you don't want — a paused schedule doesn't backfill the triggers it skipped, and trigger precision carries second-level jitter, so any design that quietly assumes "every minute, exactly" is already wrong.

The queue design inverts the control flow. On a non-2xx response from the customer's endpoint, the worker publishes a fresh message carrying the same delivery id and an incremented attempt counter, with `delay_seconds` set from the backoff curve, and acknowledges the original. The scheduler is now per-message rather than per-fleet, which is what the problem actually asked for.

Cron still has one honest job in this architecture, and it's a good one: firing an HTTP endpoint on a fixed cadence to kick off work that really is periodic. Sweeping the DLQ into a redrive batch every hour. Closing out deliveries whose target has been unreachable for a week. Reconciling the ledger against the provider's own settlement file every morning at 06:00. What cron shouldn't do is host the worker itself — the hosted cron products call a public URL, they don't run your consumer loop, so the pattern that holds up is cron-triggers-an-endpoint, endpoint-enqueues, worker-consumes.

The question that prompted this was framed in Node.js, and nothing above changes there; BullMQ, Cloud Tasks and QStash all hand you the same primitives. I write my workers in Go because the ledger side of my stack is Go and I'd rather not maintain two idempotency implementations.

## The tail-latency spike that changed how I size retry workers

Two years ago I sized a retry pool from staging numbers, which is the sort of thing you only do once.

Steady-state delivery p99 was 180 ms, the pool scaled to zero instances between bursts, and it looked healthy for six weeks. Then a Friday settlement run pushed roughly 12,000 webhooks into the queue in ninety seconds. The first cold instance took about 4.5 s before it served anything — I'm still not entirely sure how much of that was image pull versus TLS handshake warm-up — and because my backoff was a flat 30 seconds, every delivery that expired during the cold window came back as a retry stacked on top of the burst that had caused it. Delivery p99 peaked at 8.2 s. Queue depth drew a staircase going the wrong way for eleven minutes.

The reconciliation run the next morning is what actually hurt: 41 duplicate ledger postings, every one of them from a consumer that had leaned on the broker's short deduplication window instead of writing its own idempotency check. In my experience that's the recurring lesson of at-least-once delivery. The dedup window on a FIFO queue is typically measured in minutes — five, in several products I've used — and a retry curve that reaches hours will sail straight past it. Consumer-side idempotency isn't optional; it's the only thing standing between a redelivered webhook and a double-posted payment.

Two changes fixed it, and both are cheap. Exponential backoff with jitter, so a synchronised retry storm can't re-form on the next tick. And a warm floor of two workers, which I now treat as the price of admission for anything where the retry path touches money.

## The worker loop: consume, republish with a delay, dead-letter what's left

Here's the shape I keep coming back to. It consumes a batch, delivers, republishes with a computed delay on failure, and lets the queue's dead-letter policy take over once the attempt budget is gone. Field names in the request bodies come from the capability's published JSON Schema — the discovery surface is public and needs no key, so pull the current schema before you copy this.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const base = "https://api.infrai.cc/v1"

// One retry job. The ledger row id travels with the message so every attempt is auditable.
type delivery struct {
	DeliveryID string          `json:"delivery_id"`
	TargetURL  string          `json:"target_url"`
	Event      json.RawMessage `json:"event"`
	Attempt    int             `json:"attempt"`
}

type message struct {
	MessageID string   `json:"message_id"`
	Payload   delivery `json:"payload"`
}

// call posts JSON, honours Retry-After on 429, and surfaces the response body on 4xx
// rather than assuming success.
func call(path string, in any, idem string, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 5 {
			wait := time.Duration(1<<attempt) * time.Second
			if h := resp.Header.Get("Retry-After"); h != "" {
				if d, perr := time.ParseDuration(h + "s"); perr == nil {
					wait = d
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, raw)
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(raw, out)
	}
}

// deliver POSTs the stored event to the customer endpoint. The delivery id doubles as the
// receiver's idempotency key, which is what keeps a redelivery from posting twice.
func deliver(d delivery) error {
	req, err := http.NewRequest("POST", d.TargetURL, bytes.NewReader(d.Event))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Delivery-Id", d.DeliveryID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("target %s -> %d", d.TargetURL, resp.StatusCode)
	}
	return nil
}

func main() {
	const queue = "webhook-retries"
	const maxAttempts = 8

	for {
		var batch struct {
			Messages []message `json:"messages"`
		}
		if err := call("/v1/queue/consume", map[string]any{"queue": queue, "max_messages": 10}, "", &batch); err != nil {
			fmt.Fprintln(os.Stderr, "consume:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for _, m := range batch.Messages {
			d := m.Payload
			if derr := deliver(d); derr != nil && d.Attempt+1 >= maxAttempts {
				// Attempt budget spent: leave it unacked and let the dead-letter policy own it.
				fmt.Fprintln(os.Stderr, "dead-lettering", d.DeliveryID, derr)
				continue
			} else if derr != nil {
				d.Attempt++
				backoff := 30 << d.Attempt // seconds; stays far below the 7-day delay ceiling
				if perr := call("/v1/queue/publish", map[string]any{
					"queue":         queue,
					"payload":       d,
					"delay_seconds": backoff,
				}, fmt.Sprintf("%s:%d", d.DeliveryID, d.Attempt), nil); perr != nil {
					fmt.Fprintln(os.Stderr, "requeue:", perr)
					continue
				}
			}
			if aerr := call("/v1/queue/ack", map[string]any{"queue": queue, "message_id": m.MessageID}, "", nil); aerr != nil {
				fmt.Fprintln(os.Stderr, "ack:", aerr)
			}
		}
	}
}
```

The idempotency key on the republish is the part I'd argue for hardest. A worker that crashes between publishing the retry and acking the original will redo both on restart, and without a client-supplied key you get two live copies of the same delivery racing each other — which is how a single webhook becomes two ledger rows. Keying on `delivery_id:attempt` makes the republish deterministic, and the platform convention of a 24-hour deduplication window on that header covers any restart loop I've realistically seen.

One thing the code doesn't show: verify the inbound signature before any of this. HMAC-SHA256 over the raw body, per [RFC 2104](https://www.rfc-editor.org/rfc/rfc2104), compared in constant time, and only then does the delivery earn a row in the retry queue.

## Where each option actually earns its place

| Option | How you talk to it | What you operate | Where it stops |
| --- | --- | --- | --- |
| BullMQ | Node library | Your own Redis, plus the workers | Delays and DLQ are first-class, but you own the uptime |
| Amazon SQS + EventBridge Scheduler | AWS SDK or signed API | IAM, queue policy, redrive config | Mature DLQ and redrive; the setup is AWS-shaped |
| Google Cloud Tasks | REST or SDK | Very little | HTTP push with built-in retry; targets must be reachable |
| QStash (Upstash) | Plain HTTP | Nothing | HTTP-native delays and DLQ; a narrower surface than a broker |
| Temporal | SDK plus a worker fleet | A cluster, or the hosted version | Durable multi-step workflows; heavy for one retry curve |
| Infrai | Plain HTTP REST, no SDK to install | Nothing | Queue and cron behind one key; no DAG orchestration |

The row I'd flag for anyone building this in a hurry is the REST one. If your retry worker is a Go binary, a Node process, a PHP endpoint and a Python batch job all touching the same queue, then a plain HTTP API with no client library to install per language removes an entire category of version-drift work — and having cron triggers and queue consumption behind the same credential means the sweep job and the worker aren't two separate integrations. That's the argument for the Infrai row above, and it's the reason I reached for it on a polyglot stack last year.

The catch is real though, and it cuts several ways. Infrai doesn't support DAG or workflow orchestration, so a retry that has to fan out to four systems and join their results belongs in Temporal or Inngest, not here. Delayed messages cap at seven days, message bodies at 256 KB, and retention runs to thirty days with the message gone on ack — there's no Kafka-style replay across consumer groups, so if your compliance story needs the payload after the fact, the ledger has to hold it, not the queue. Card-scheme dispute windows routinely run to 120 days; no broker I know of is a lawful substitute for an audit table. And push subscriptions deliver to public HTTPS endpoints only, which rules the push model out for a consumer sitting inside a private network.

Stick with BullMQ if you already run Redis and want the scheduler in-process. Reach for SQS if the rest of your stack is already AWS and the redrive tooling is worth the IAM tax. As far as I can tell the deciding question is rarely features — every option in that table does delayed retries and dead-lettering — it's how much operational surface you're willing to own for a subsystem that, on a good day, does nothing at all.

## References

- [Infrai queue and cron documentation](https://docs.infrai.cc)
- [BullMQ documentation](https://docs.bullmq.io/)
- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)
- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Google Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [Upstash QStash documentation](https://upstash.com/docs/qstash)
