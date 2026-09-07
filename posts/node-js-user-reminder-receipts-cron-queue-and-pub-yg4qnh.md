# Node.js User Reminder Receipts: Cron Queue and Public Webhook Endpoints

**Short answer:** keep the reminder schedule and its delivery ledger in durable storage, use cron only to find work that is due, use a queue to isolate sending, and make every email, SMS, and public webhook event idempotent. That arrangement makes time-zone policy explicit and leaves an audit trail when a reminder is questioned later.

A reminder system looks like a timer until the first retry, reschedule, or daylight-saving transition turns it into an accounting problem. The central constraint is not that a process must wake up at 09:00; it is that a system must be able to explain, for one user and one intended notification, which local-time rule produced the instant, which attempt was authorized to send, and which later receipt changed the recorded outcome. A worker may run twice. A broker may redeliver. An external delivery provider may report an event after a later event. The design has to remain correct under those ordinary conditions.

Keep the clock boring.

## How should Node.js user reminders use cron, queues, delayed messages, time zones, and a public webhook endpoint?

Start with a reminder row, not a timer in a Node.js process. The row needs a stable reminder identifier, a channel, the user's IANA time-zone identifier, the requested local wall time, a computed UTC `fire_at`, a state, and a revision number. A reschedule updates that row and increments the revision. It does not try to locate a long-lived delayed message whose identity and cancellation semantics are now part of the business model.

The cron job should run on a short, fixed cadence in UTC and claim a bounded set of rows where `fire_at` is due. `crontab(5)` documents `CRON_TZ`, which is useful for interpreting a machine's cron entries, but it is not a per-user scheduling model. A local offset such as `-05:00` is also insufficient: it describes one instant, while an IANA zone name describes the calendar rules needed to resolve future local times. Decide and document what happens to a local time that is skipped during a forward clock change and to one repeated during a backward change. There is no universal answer; a billing reminder may choose a different policy from a wellness nudge. The important property is that the chosen policy is stored with enough context to reproduce it.

The scanner then publishes a small work item containing the reminder ID and revision. A queue is valuable here because it separates due-time selection from slow, rate-limited external I/O. A short delay can smooth a burst and create a retry interval, but the durable table remains the schedule of record. A delay of weeks inside a message broker makes rescheduling, inspection, and reconciliation substantially harder, because the business schedule has moved into transient transport state.

This split also makes deployment less magical. Several scanner instances can run if their claim is transactional, and an old queued item becomes harmless when the worker compares its revision with the current row before sending. The scanner need not be perfectly punctual; it needs a bounded lag that is observable. Record the target instant, claim time, publish time, and actual send time separately. Those timestamps answer different questions during an audit.

## The durable boundary is the send ledger

Queues generally provide at-least-once delivery, which is the right assumption even when normal operation appears quieter. RabbitMQ's acknowledgement documentation, for example, explains that an unacknowledged delivery can be requeued when its consumer or connection goes away. A duplicate work item is therefore expected input, not evidence that the queue is misbehaving.

The useful target is exactly-once effect, rather than an impossible promise that a message will be observed exactly once. Give each intended effect a deterministic key such as `reminder_id + channel + revision`, enforce uniqueness for that key in the database, and preserve the key through every retry. The database transaction that claims the effect is the authority; the queue acknowledgement comes after the durable state is safe.

| Concern | Durable record | Why it matters |
| --- | --- | --- |
| Scheduling | reminder ID, local-time rule, zone, UTC instant, revision | Reschedules and daylight-saving decisions remain explainable. |
| Claiming | idempotency key, attempt number, claim timestamp | Only one logical delivery may advance to the outbound call. |
| Outcome | provider message ID, terminal status, receipt timestamp | A later reconciliation can distinguish accepted, delivered, bounced, and unknown. |
| Retry | next eligible time and failure classification | Retrying does not erase the prior attempt. |

The hard case is the gap between an outbound request and recording its receipt. A process can stop after the request reaches the provider but before local storage records the result. Retrying with the same idempotency key lets a provider that supports request idempotency collapse the repeated request; without that support, the ledger still makes the ambiguity visible for reconciliation. Do not mark a notification as delivered merely because an HTTP request was accepted. An HTTP `202` or `200` is an acknowledgement of a request, not proof that an email reached an inbox or that an SMS reached a handset.

Here is a focused Go-shaped worker boundary. The interfaces are intentionally generic: scheduling correctness should not depend on a particular broker or notification service.

```go
type ReminderJob struct {
	ReminderID string
	Revision   int64
	Channel    string
}

type Ledger interface {
	Claim(ctx context.Context, reminderID, channel string, revision int64) (key string, alreadyFinal bool, err error)
	RecordAccepted(ctx context.Context, key, providerID string) error
}

type Sender interface {
	Send(ctx context.Context, job ReminderJob, idempotencyKey string) (string, error)
}

func Deliver(ctx context.Context, ledger Ledger, sender Sender, job ReminderJob) error {
	key, alreadyFinal, err := ledger.Claim(ctx, job.ReminderID, job.Channel, job.Revision)
	if err != nil {
		return err
	}
	if alreadyFinal {
		return nil
	}

	providerID, err := sender.Send(ctx, job, key)
	if err != nil {
		return err
	}
	return ledger.RecordAccepted(ctx, key, providerID)
}
```

The code alone cannot create exactly-once delivery. It makes the ownership boundary visible: `Claim` must be an atomic durable operation, and `RecordAccepted` must retain the provider identifier and attempt history rather than overwriting them. In a payment or ledger environment, this is familiar discipline. An event must be attributable, replayable, and reconcilable; a notification side effect deserves the same treatment.

## A webhook is an event intake, not a status overwrite

Delivery receipts, bounces, complaints, and opt-outs usually arrive through a public webhook endpoint. Treat the endpoint as an untrusted event intake. Verify the sender's signature over the raw request body before interpreting the payload, place the verified event in durable storage under the provider event ID, and return quickly. A separate worker can then derive a reminder's current presentation state from the ordered facts it has received.

Out-of-order events are normal enough to design for. A terminal receipt may arrive before a less informative acceptance event, and a provider may retry the same webhook after a timeout. A unique constraint on the event ID handles the replay. A transition rule that never moves a terminal record backward handles arrival order. If the signature is invalid, reject the request; if the event is already recorded, acknowledge it without creating a second audit entry. A conflict that surfaces as HTTP `409` inside an internal API is useful state, not something to paper over with a blind retry.

Keep consent and retention beside this pipeline. SMS and email reminders can fall under contractual, consumer-protection, privacy, and sector-specific controls that differ by recipient, jurisdiction, and message purpose. The evidence here is incomplete for any particular product, so compliance counsel must determine the applicable rules and the required retention period. Engineering can still provide the materials counsel needs: a versioned template, a consent reference, sender identity, timestamps, and the complete sequence of delivery facts.

## Choosing the timing mechanism is a horizon and recovery decision

Cron scans, short delayed messages, and in-process timers each have a valid but narrow role.

| Mechanism | Appropriate use | Limitation to accept |
| --- | --- | --- |
| Database schedule plus cron scan | User reminders with reschedules, audits, and horizons of days or months | Requires indexes, a claim transaction, and backlog monitoring. |
| Short queue delay after a claim | Rate smoothing and controlled retries | It is transport state, so it should not be the sole schedule. |
| In-process timer | Local development or ephemeral work | A restart discards the timer state. |

The catch is operational weight. A durable schedule adds schema migrations, a clock-resolution policy, metrics, and a reconciliation job; for a disposable internal reminder with no compliance requirement, that may be disproportionate. Stick with a simple in-process timer only when losing work on restart is acceptable and the deadline is not a user commitment. For externally visible email and SMS reminders, the ledger is usually the smaller long-term cost because it provides an answer when somebody asks what occurred.

I'm not sure a single service-level objective fits every channel. A password-reset email, a payment due notice, and an appointment reminder have different harm from lateness and duplication. Define separate lag, duplicate-suppression, receipt-completeness, and webhook-verification measures, then alert on their changes rather than on raw queue depth alone. Queue depth can remain low while a scheduler is selecting the wrong time zone.

## Roll out the reminder path with reconciliation from day one

Introduce the path in stages. First write schedule rows and compute `fire_at` without sending, then compare intended local times against independently reviewed fixtures for ordinary dates and clock transitions. Next enable a small cohort with the ledger and webhook intake active, and reconcile every accepted send against a terminal receipt or an explicitly open investigation state. Finally, scale the scanner claim limit only after observing backlog age, duplicate claims, and receipt latency across a deploy.

Do not delete old attempts during the rollout. Preserve them as append-oriented evidence, and make a repair job select the oldest unresolved claims by channel and time zone. This is slower than treating the latest status as truth. It is also how a system stays explainable when retries, reschedules, and delayed messages meet real calendars.

## References

- https://man7.org/linux/man-pages/man5/crontab.5.html
- https://www.rabbitmq.com/docs/confirms
