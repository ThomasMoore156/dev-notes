# Email API vs SMTP Relay in 2026: Choose API for Low-Volume SaaS Password Resets

Short answer: for a low-volume SaaS sending password reset emails, choose a transactional email API when it gives you templates, a suppression list, delivery tracking, and documented US/EU data handling through one small integration; choose an SMTP relay when an existing mail abstraction already owns those controls and changing it would create more work than it removes. The cheapest option in 2026 is the one with the lower measured integration and operating burden at your actual volume, not automatically the one with the smallest per-message quote.

That decision also needs a boundary. An e-commerce team that later sends generated reports as attachments may outgrow an API chosen only for tiny reset messages, while a team with a mature SMTP pipeline may gain nothing from another delivery adapter. This note treats password resets as security-sensitive state transitions, then uses the attachment case to expose where the apparently simple choice stops being simple.

## Integration effort is the dominant cost at low volume

Start with a ledger, not a price page. Record four entries for each option: initial engineering hours, recurring operator hours, message charges at current volume, and the expected cost of investigating one ambiguous delivery. Insert your own vendor quote and internal labor rate; don't borrow somebody else's traffic curve. A useful comparison is `total = build + operate + send + investigate`, with every input dated and owned.

For example, a planning worksheet might assign 8 engineering hours to an API adapter, 24 hours to adding template rendering, suppression checks, and event ingestion around a bare relay, then 1 hour per month to reconcile delivery events. Those are design assumptions, not market benchmarks. Replace them during a short proof of concept. The point is that the team can see which term dominates: if the 24-hour integration estimate is the largest line, arguing over a fractional message rate won't change the decision; if both paths already exist internally, the incremental build term may be zero and the relay can win. I'm not sure which term dominates in your system until those local numbers exist, and any confident universal answer to “cheapest” is hiding that uncertainty.

Count retention too. Keeping full message bodies and attachments makes an investigation easier, but expands the sensitive-data footprint and the work required to honor a deletion policy. Keeping only a provider message identifier, internal notification identifier, template version, recipient hash, timestamps, and normalized state usually supports reconciliation without preserving reset links or generated reports. The catch is explicit: once the body is discarded, an operator cannot reconstruct exactly what a user saw. Preserve a versioned template and immutable input facts only when that reconstruction is a real audit requirement, set a documented retention period, and have counsel or the responsible compliance owner approve the US/EU data path rather than treating a region selector as proof of compliance.

Keep less. Accept the forensic cost deliberately.

## What should a low-volume SaaS require from a simple password reset email API?

The minimum contract is broader than “accepted my request.” A password reset workflow needs one application-level idempotency key per reset notification, a stable internal notification ID, template versioning, suppression behavior that is visible before or after submission, and delivery events that can be correlated without storing the token. “Accepted” means the delivery system took responsibility for processing; it does not prove that the intended person read the message. Model those as different facts.

I use an exactly-once mindset here even though the network cannot promise exactly-once delivery: the application makes one durable decision, records it transactionally, and retries an idempotent dispatch operation until it has a terminal or reviewable outcome. A second click on “forgot password” may represent a new security action and therefore a new notification ID, but a worker retry for the same outbox row must reuse the original idempotency key. That distinction prevents a timeout from becoming two reset emails while preserving the user's ability to request another legitimate reset.

Templates belong in the audit trail. Record the template version and locale that were selected, but never log the reset token or place it in an event payload. Treat suppression as a named outcome such as `suppressed`, not as an exception that disappears into a retry queue. Delivery tracking should likewise map provider-specific events into a small internal vocabulary: `accepted`, `delivered`, `temporarily_deferred`, `permanently_failed`, and `suppressed`. Your mileage may vary on the exact names; the invariant is that every transition is attributable, timestamped, and idempotent.

US/EU requirements need concrete questions: where recipient data and event metadata are processed, where they are retained, which subprocessors participate, what deletion mechanism exists, and whether the contractual terms match the application's obligations. The CTIA guidance in the references concerns SMS/MMS messaging, so it should not be presented as an email compliance rule. It becomes relevant only if the recovery design adds SMS as a separate channel, at which point consent, messaging practices, and channel-specific controls deserve their own review.

## API versus relay: compare ownership, not syntax

An email API commonly exposes structured fields for a template identifier, variables, and a client reference; an SMTP relay accepts a composed message through the mail protocol. Either transport can sit behind a sound internal port. The real comparison is ownership: does the delivery service own template rendering, suppression, and event normalization, or does your application and platform team own them?

| Option | Integration boundary | Best fit | Main limitation |
| --- | --- | --- | --- |
| Transactional API | Structured request and correlated events | A small team that wants one managed path for templates, suppression, and tracking | Provider-specific fields and data terms require review |
| SMTP relay | Composed message over the mail protocol | A team with an established internal mail platform | The application or platform must own missing lifecycle controls |

Choose the API path when the proof of concept confirms that one integration covers the required template lifecycle, suppression visibility, correlated delivery events, and acceptable regional data terms. This reduces the amount of mail-specific machinery the application team must build. It doesn't remove the need for an outbox, idempotency, access control, redaction, or reconciliation. A delivery provider is a downstream system, and downstream acknowledgement must never be allowed to rewrite the business fact that a reset was requested.

Stick with SMTP when the organization already has a reviewed relay abstraction that supplies the same controls, or when portability at the protocol boundary is more valuable than structured provider features. SMTP is also the safer fit when policy requires fully local template rendering and the existing platform already captures correlated outcomes. The limitation is operational ownership: if suppression and tracking have to be invented anew inside one low-volume service, the superficially smaller integration has merely moved work into less visible code.

The adjacent attachment workload is a useful test. A generated e-commerce report can be larger, can contain commercially sensitive data, and may need a different retention policy from a password reset. Verify attachment size rules, encoding behavior, malware-scanning responsibility, timeout limits, and whether a signed download link is preferable. If those answers force a second delivery design, keep the reset adapter narrow rather than distorting it into a universal messaging layer.

## A Go boundary for idempotent dispatch and audit

The application should depend on a small capability interface, not on transport vocabulary. The example below deliberately contains no vendor route and no reset token. `NotificationID` identifies the durable business record, while `IdempotencyKey` remains stable across worker retries.

```go
package notification

import (
    "context"
    "errors"
    "time"
)

type ResetEmail struct {
    NotificationID string
    IdempotencyKey string
    Recipient      string
    Template       string
    TemplateVersion string
    Locale         string
    ResetURL       string // Pass to the sender; never write this value to logs.
}

type Receipt struct {
    ProviderMessageID string
    AcceptedAt        time.Time
}

type Sender interface {
    SendReset(ctx context.Context, msg ResetEmail) (Receipt, error)
}

type AuditStore interface {
    MarkAccepted(ctx context.Context, notificationID, providerMessageID string, at time.Time) error
}

func Dispatch(ctx context.Context, sender Sender, audit AuditStore, msg ResetEmail) error {
    if msg.NotificationID == "" || msg.IdempotencyKey == "" {
        return errors.New("notification ID and idempotency key are required")
    }

    receipt, err := sender.SendReset(ctx, msg)
    if err != nil {
        return err // The outbox worker retries with the same idempotency key.
    }

    return audit.MarkAccepted(ctx, msg.NotificationID, receipt.ProviderMessageID, receipt.AcceptedAt)
}
```

This boundary is intentionally incomplete in one important way: `MarkAccepted` can fail after the downstream system accepts the message. The worker must reconcile that uncertainty by retrying with the same key and by making the audit transition idempotent; generating a fresh key would risk a duplicate. In a production design, the outbox row, attempt records, normalized provider events, and state-transition uniqueness constraints form the evidence chain. Test the awkward sequence: dispatch accepted, process interrupted before the local update, same row retried, one logical acceptance retained. Also test duplicated and out-of-order events, a suppressed recipient, a permanent failure, and a template version removed between enqueue and dispatch.

Don't log secrets.

Deploy the adapter behind a narrow configuration switch, send only to controlled test recipients first, and reconcile every test notification ID against the accepted and delivery records before expanding traffic. Observability should count transitions and their age rather than expose recipient addresses. Alert on outbox age, missing event correlation, and repeated permanent outcomes; those signals identify a stuck workflow without turning logs into a second customer database.

## Decision rule and the limits of a simple API

Choose the transactional API after a time-boxed integration exercise demonstrates fewer owned components for templates, suppression, tracking, and regional handling than the existing SMTP path. Keep SMTP when those capabilities are already supplied by a platform abstraction, when local rendering is mandatory, or when the API's data-processing terms and attachment boundaries do not fit. This is a directional choice with conditions, not a product ranking.

The cheapest candidate can therefore lose. Reject any option that cannot show an idempotent request boundary, exportable correlation data, explicit suppression semantics, controllable retention, and a reviewed US/EU processing path, even if its send line is smaller. For the surviving candidates, compare the dated worksheet totals, repeat the exercise at a plausible higher volume, and record the decision plus its assumptions in the repository. Revisit it when volume, attachment use, regional scope, or operator time changes.

There is no universal winner.

## References

- Amazon Simple Email Service documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- CTIA messaging interoperability principles and best practices for SMS/MMS: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms

## Further reading

The first reference documents one transactional email service surface; the second applies only to an SMS recovery channel. Neither substitutes for reviewing the selected service's current contract, data-processing terms, and technical documentation.
