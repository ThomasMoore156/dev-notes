# SMS OTP Delivery After Sender Registration: 1 Deadline Despite Carrier Filtering

A media subscriber's password-reset window can close before the message arrives. TL;DR: SMS OTP delivery can fail because of unregistered senders, carrier filtering, handset issues, or temporary routing delays; plan sender registration, bounded retries, and an email fallback, while keeping one application-owned expiry deadline across the entire recovery attempt. A late delivery observation must never extend that deadline or authorize a password change.

Delivery remains uncertain. Expiry must not.

The useful engineering question is therefore temporal: which events are allowed to change the recovery request after its validity window ends? For a short-lived reset, a transport integration should improve the chance that a subscriber receives a usable credential, while the authorization boundary remains correct when transport evidence arrives late. That distinction makes reliability assessable without assuming instant delivery or promising exactly-once receipt.

## Why can SMS OTP delivery fail after sender registration?

Treat the message and the authority it carries as separate objects. The message can still be observed, reconciled, and included in an audit trail after the reset request has expired, but the credential must be rejected by the application. Recording a later delivery outcome is an accounting operation; reopening authorization would be a security decision for which the delivery event supplies no justification.

Consider a synthetic media-account example with a 90-second reset lifetime. At second 0 the application creates the recovery intent; at second 60 the subscriber requests a permitted fallback; at second 90 the original deadline passes; at second 95 a transport observation becomes available. These numbers are proposed application settings and an illustrative event sequence, not a vendor default or a measured carrier delay. Both channels share the deadline. Starting a new 90-second timer when fallback begins would silently turn a short-lived reset into a longer-lived credential, even though the product still displays the original expiry promise.

The trap is timer renewal.

Registration does not eliminate this problem. An appropriately registered sender can still encounter filtering, a handset issue, or temporary routing delay, so US and EU delivery investigations must separate sender eligibility from subsequent transport outcomes. Shared routes are a reasonable subject for investigation, but a missing message alone does not establish that route sharing caused filtering. Preserve sender context and the message identifier; avoid converting an unknown outcome into an asserted cause.

Compliance has its own boundary: Twilio's US A2P 10DLC documentation addresses that US registration context, rather than establishing permission for every EU destination. Likewise, an observed SMS delivery is not proof of legal intake verification or authority to act for another person. A media password reset should grant only the authority its own authentication policy establishes.

## Make the deadline immutable across channel changes

Persist the recovery identity, its original expiry, the identities of associated dispatch attempts, and its consumption state. Poll message status and events, attaching observations to those attempts without allowing the observer to rewrite the expiry. This gives reconciliation a place to put delayed evidence after authorization has closed, which is preferable to either discarding the evidence or treating it as a fresh reset request.

The following complete Go program isolates this temporal rule with synthetic inputs. Its fields are application-owned values, not a provider response schema. It models the final authorization decision after credential validation and other security checks have succeeded; it does not generate a credential or send a message.

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

type Reset struct {
    mu       sync.Mutex
    expires  time.Time
    consumed bool
}

func (r *Reset) consume(now time.Time) string {
    r.mu.Lock()
    defer r.mu.Unlock()

    if !now.Before(r.expires) {
        return "expired"
    }
    if r.consumed {
        return "already_consumed"
    }
    r.consumed = true
    return "authorized"
}

func main() {
    issued := time.Date(2026, time.November, 19, 12, 0, 0, 0, time.UTC)
    deadline := issued.Add(90 * time.Second)
    early := &Reset{expires: deadline}
    boundary := &Reset{expires: deadline}
    late := &Reset{expires: deadline}

    fmt.Println("media-email-at-60:", early.consume(issued.Add(60*time.Second)))
    fmt.Println("media-sms-at-70:", early.consume(issued.Add(70*time.Second)))
    fmt.Println("media-at-90:", boundary.consume(deadline))
    fmt.Println("media-sms-at-95:", late.consume(issued.Add(95*time.Second)))
}
```

The results are `authorized`, `already_consumed`, `expired`, and `expired`, respectively. Equality with the deadline is rejection, not an extra second of grace.

The mutex makes consumption atomic within this demonstration process. A deployed backend needs a durable transaction that atomically validates the recovery state, consumes the credential, and applies the password change, because separate workers and process restarts do not share this mutex. The supplied `now` value is a deterministic demonstration input; a production decision must use a trusted application or database clock. Keep the reset secret out of audit logs, and retain the recovery identity, attempt identity, decision time, and reason under an explicit retention policy.

This is an exactly-once mindset applied to an account mutation. It is not an assertion that an SMS network delivers exactly once.

Dispatch idempotency solves a related, narrower problem: retrying one logical submission should retain its dispatch identity rather than create another intended send. A deliberate resend is a separate policy decision, still constrained by the original expiry, suppression, lockout, and a centrally enforced attempt budget. Rate-limit the resend interface across channels; changing from SMS to email must not erase the subscriber's existing abuse history. When a transport returns HTTP 429, honor Retry-After where present and use bounded exponential backoff, abandoning further dispatch when the remaining reset lifetime cannot accommodate it.

There is a trade-off. A strict deadline can reject a legitimate subscriber's late code, but extending it in response to transport delay makes credential validity depend on the least predictable part of the system. Offer a new, explicitly authorized recovery request after expiry, subject to the same suppression and lockout rules. Do not silently resurrect the old one.

Country eligibility, geofencing, and country-based anti-abuse or spending circuit breakers belong in application policy. For email fallback, own the credential issuance and consumption workflow, use the email transport for delivery, and keep expiry enforcement independent of message cancellation. A password-reset design should remain safe even if a message already in transit cannot be withdrawn.

## Select a transport without delegating authorization

The deadline rule narrows the supplier decision: choose a delivery integration that fits the required destinations and observation workflow, while retaining the same authorization semantics. There is no comparative delivery-rate measurement here from which to rank providers. In particular, an email option answers a different part of the workflow from an SMS registration reference.

| Option | Evidence relevant to this decision | Fit and limit |
| --- | --- | --- |
| Twilio | Its US A2P 10DLC documentation directly addresses a sender-registration context. | A relevant SMS candidate when assessing US sender eligibility; that source alone cannot establish EU compliance or on-time delivery. |
| Amazon SES | Its official documentation covers the email service. | A relevant candidate for the fallback delivery leg; email transport selection leaves reset expiry and single consumption with the application. |
| Amazon SNS | A third candidate for a separate messaging-service assessment; no comparative delivery evidence is established in this note. | Retain it on the shortlist, but obtain destination-specific registration and observation documentation before assigning it the reset workflow. Its inclusion is not a claim of feature parity or a delivery ranking. |

Infrai exposes one REST API with a single key and a single bill across 295 routes in 20 modules: swapping vendors behind a capability does not change the calling code, and plain HTTP requires no SDK installation. For the reset workflow, one key and one bill mean fewer credentials to configure and fewer invoices to reconcile across SMS and email; the broader route count is not evidence of better delivery. Use polling-based observation and an application-owned email credential workflow for that integration. A stable capability boundary does not imply identical carrier outcomes, and polling cadence must fit within the recovery deadline.

To connect observation to the deadline rule, this standalone Go program retrieves an existing SMS status without assuming undocumented response fields. Set `INFRAI_API_ORIGIN` to the service's HTTPS API origin, `INFRAI_API_KEY` to the credential, and `SMS_ID` to the existing message identifier. Its illustrative 10-second observation budget does not modify the recovery deadline; the caller must further restrict that budget when less validity remains. It performs a read, so it creates no new dispatch to deduplicate.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func observe() error {
    origin := strings.TrimRight(os.Getenv("INFRAI_API_ORIGIN"), "/")
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_ID")
    u, err := url.Parse(origin)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.User != nil || u.Path != "" || u.RawQuery != "" ||
        u.Fragment != "" || key == "" || id == "" {
        return fmt.Errorf("set HTTPS API origin, credential, and SMS identifier")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 3 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        path := strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1)
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            origin+path, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 65536))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if res.StatusCode >= 200 && res.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return fmt.Errorf("status %d: %s", res.StatusCode, body)
        }
        wait := time.Second * time.Duration(1<<attempt)
        retry := res.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds > 0 {
            if seconds > 10 {
                return fmt.Errorf("retry exceeds observation budget: %s", body)
            }
            if delay := time.Duration(seconds) * time.Second; delay > wait {
                wait = delay
            }
        } else if date, err := http.ParseTime(retry); err == nil {
            if delay := time.Until(date); delay > wait {
                wait = delay
            }
        }
        timer := time.NewTimer(wait)
        select {
        case <-ctx.Done():
            timer.Stop()
            return fmt.Errorf("observation budget exhausted: %w; last response: %s",
                ctx.Err(), body)
        case <-timer.C:
        }
    }
    return fmt.Errorf("observation attempts exhausted")
}

func main() {
    if err := observe(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The selection criterion is concrete: reject any proposed integration design that renews the reset timer upon resend, fallback, provider substitution, or a delayed status observation. That rule can be checked before comparing delivery outcomes under real destination and sender conditions.

## Roll out with delayed evidence before live sends

Begin with synthetic event replay: a message observed after expiry, two concurrent consumption requests, and a fallback followed by a late SMS. Require the audit trail to explain each outcome without recording the credential, and require one account mutation at most. Then enable a limited destination cohort with sender eligibility checked, bounded polling, suppression, and lockout enforcement in place.

Measure usable recovery completion before the original deadline separately from transport observations. Expand only after reconciling both records; a higher count of accepted submissions cannot, by itself, establish more reliable password recovery.

## References

The registration reference supports the US sender discussion; the email-service reference supports evaluation of the fallback leg. Neither supplies comparative delivery benchmarks. The third candidate remains an explicit evidence gap rather than an assumed equivalent.

## Sources

- [Twilio, US A2P 10DLC compliance documentation](https://www.twilio.com/docs/messaging/compliance/a2p-10dlc)
- [Amazon SES, official documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
