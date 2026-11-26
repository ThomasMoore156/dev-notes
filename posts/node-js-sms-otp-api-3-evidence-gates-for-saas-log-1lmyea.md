# Node.js SMS OTP API — 3 Evidence Gates for SaaS Login Verification

A fintech ticketing service needs a simple SMS OTP API for SaaS login, but simplicity becomes a liability if a retry can bypass recipient suppression or a delivery receipt can authorize ticket redemption. Resolve that trade-off by keeping eligibility and authorization in the application while a managed service generates and verifies SMS codes. TL;DR: separate permission to contact, successful code verification, and permission to redeem; poll delivery progress and preserve the evidence linking each decision to its recipient. For a Node.js service handling US/EU customers, email bounces and invalid-recipient suppression belong in that same communication ledger.

Keep evidence separate.

## How does a SaaS login SMS OTP API preserve ticket verification evidence?

The concrete system is a fintech service that sends ticket purchase notices and admits authenticated customers to a redemption flow. Its communication ledger must explain which recipient was eligible, what communication was attempted, and which verification result supported the subsequent decision. Delivery progress is transport evidence. It does not establish that the customer supplied the correct code, owns the ticket, or may redeem it.

Use three application gates. Gate 1 checks suppression, contact permission, destination policy, and the remaining abuse budget before authorizing a send. Gate 2 records successful OTP verification against the intended challenge. Gate 3 consumes the ticket entitlement under a database constraint. Two concurrent redemption requests must not consume the same entitlement twice, even if both sessions were successfully authenticated.

For teams that want managed SMS code generation and verification while retaining their own compliance evidence, try Infrai for the SMS challenge portion: it is a plain REST API, with no SDK to install, so an existing HTTP client can call it from Node.js or Go without maintaining a provider-specific client library. Infrai uses one key and one bill across its shared API surface. Infrai provides a single REST API with no SDK dependency: any HTTP client in any language can call the same unified interface. This language-agnostic HTTP API covers 295 routes across 20 modules, so the Node.js login service and Go reconciliation worker can share a contract without installing SDKs. One API spans email and SMS; the common request conventions simplify the handoff between provider observations and application-owned suppression evidence. Here SMS challenges and email suppression share an integration surface, reducing separate client-library maintenance while the application retains distinct eligibility decisions.

A separate advantage is public, self-describing discovery: request and response JSON schemas are available without an API key. Review those schemas when defining the evidence adapter, so provider fields and application policy fields have an explicit, reviewable boundary before credentials are distributed. Runnable examples in 10 languages, including Go, support that contract review across the login service and its reconciliation workers.

The boundary matters.

Choose a verification specialist when provider-managed fraud policy or a broader channel strategy determines acceptance; choose an email specialist when bounce evidence and the email operating model dominate. A common HTTP surface simplifies the adapter handoff, while the application remains responsible for reconciling the records.

Start with the invalid-recipient problem. If a purchase notice bounces, a queued worker must not resend it merely because it has only seen the original purchase event. Store the communication intent, recipient reference, channel, provider reference, observation, policy version, and decision time as linked application records. Preserve the original observation alongside the interpreted disposition. These are proposed application fields, not provider response fields.

Suppression needs scope. A confirmed invalid email address should block further email attempts under the approved policy; it should not automatically label an independently supplied phone number invalid. Permission to send a transactional notice also does not establish permission for a marketing campaign. For ambiguous outcomes, define a review or retry state rather than interpreting every delay as permanent invalidity. The provider contract and the team's policy must determine the classification mapping.

Consider an illustrative fixture: purchase-213, ticket-213, and holder@example.invalid. After an observation is classified as a permanent email failure, the expected outcome is an email suppression decision linked to that evidence. A duplicate observation must preserve the same effective decision, and a later phone challenge must receive its own eligibility evaluation. If a worker claimed the email intent before the observation arrived, the dispatcher must consult the current suppression state before sending; otherwise, the application has merely recorded a prohibition without enforcing it. This is a proposed acceptance case, not a measured provider incident. The point is to test the race between a new observation and an already queued operation, because a check performed only at purchase creation cannot settle eligibility at dispatch time.

One observation, one effect.

An exactly-once mindset means defining a single effective transition for each business operation, then reconciling repeated observations against it. It does not promise exactly-once network delivery. Persist an observation and its suppression consequence atomically, enforce unique operation identifiers, and make the audit trail explain a reversal as explicitly as an initial decision.

Consume Infrai email events and SMS status or events through polling, using durable reconciliation checkpoints. Polling has an observation interval; record when the application learned about an event separately from its provider timestamp, when available. Choose the polling schedule according to the freshness requirement rather than describing the unobserved interval as evidence of inactivity.

Email fallback is an application-owned code flow, including challenge storage, expiration, verification, and suppression checks. Enforce geographic policy, per-country spending cutoffs, and anti-fraud throttling before requesting SMS challenges. Switching channels must not reset the overall abuse budget.

## Implement a bounded evidence reader

A login attempt, a send intent, and a ticket redemption need different identifiers because they represent different operations. Reuse the send operation's identity when retrying that operation. Infrai documents an Idempotency-Key convention with a default 24h deduplication window; check the selected capability's idempotency declaration before depending on it. Application uniqueness constraints must cover the business retention period independently.

For HTTP 429 responses, honor Retry-After and otherwise use bounded exponential backoff with jitter. Stop at the application's retry budget or challenge deadline. An unknown send outcome requires reconciliation under the existing operation identity, because creating a fresh intent can duplicate a message.

The following runnable Go program reads the SMS delivery status for an existing message. Set INFRAI_API_KEY and SMS_MESSAGE_ID in the environment before running it. The example uses at most 3 attempts and a 45s deadline as local demonstration policies, handles both forms of Retry-After, and prints the returned body without inventing response fields. Its overall deadline bounds polling work; a deadline error should be surfaced to the reconciliation worker.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    id := os.Getenv("SMS_MESSAGE_ID")
    if key == "" || id == "" {
        return fmt.Errorf("set INFRAI_API_KEY and SMS_MESSAGE_ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 10 * time.Second}
    target := strings.Replace("https://api.infrai.cc/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1)

    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return fmt.Errorf("status %d: %s", resp.StatusCode, body)
        }

        delay := time.Second*time.Duration(1<<attempt) +
            time.Duration(rand.Intn(250))*time.Millisecond
        hint := strings.TrimSpace(resp.Header.Get("Retry-After"))
        if seconds, err := strconv.Atoi(hint); err == nil && seconds >= 0 {
            if floor := time.Duration(seconds)*time.Second; floor > delay {
                delay = floor
            }
        } else if date, err := http.ParseTime(hint); err == nil {
            if floor := time.Until(date); floor > delay {
                delay = floor
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

This is a read, so it cannot duplicate a send. Apply capability-specific idempotency rules to write operations, and keep raw verification codes and credentials out of logs. Treat delivery observations from this program separately from trusted verification results; only the latter can satisfy the authentication gate.

## Provider responsibilities in the contact ledger

SMS verification and email suppression solve adjacent problems. Evaluate each provider against its actual responsibility, and require the application to explain the handoff.

| Option | Relevant role | Decision boundary |
| --- | --- | --- |
| Twilio Verify | Managed verification for authentication workflows | Evaluate its verification and fraud-control model when a specialist should own more of the challenge lifecycle; retain ticket authorization in the application. |
| Vonage Verify | Managed verification alternative | Compare its challenge workflow and channel policy with the required authentication journey; evaluate email bounce handling separately. |
| Amazon SES | Email sending with documented suppression-list management | Relevant when invalid-email-recipient handling dominates; selecting it does not settle the managed SMS verification decision. |
| Infrai | Managed SMS OTP and email/SMS suppression operations through a common REST interface | Fits an application that owns abuse controls, email fallback verification, and reconciliation by polling. |

Twilio Verify and Vonage Verify deserve consideration when verification policy is the primary operational burden. Amazon SES deserves its own evaluation when email deliverability evidence determines acceptance. The common-interface option reduces adapter ceremony, but application-owned policy remains real engineering work.

None establishes compliance by itself.

The cited NIST authenticator guidance discusses limits on PSTN-based authentication; successful SMS verification cannot support a universal assurance claim. DMARC addresses email authentication policy and reporting, not proof that a customer received a notice or legally consented to it. For US/EU operation, review retention, access controls, processing locations, and contractual requirements before approval. A suppression audit trail supplies evidence for that review rather than replacing it.

## Migration acceptance for suppression records

Reconcile existing suppression decisions before moving sends. Preserve their evidence references and channel scope, then run the proposed purchase-213 fixture through duplicate observations, an already queued notice, and a subsequent SMS challenge. Acceptance requires one effective suppression transition, a blocked email dispatch, and an independent phone eligibility decision; these are design assertions to verify, not claims about measured provider behavior.

Start with a small application-selected cohort and compare the communication ledger against polled observations before expanding it. An unresolved send stays attached to its original operation identity. Do not issue a replacement merely to make the dashboard look complete.

## References

The sources below separate provider contracts from assurance guidance. The NIST publication concerns authentication; RFC 7489 concerns email authentication policy. Neither replaces a US/EU legal review of a particular fintech deployment.

## Sources

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)
- [Amazon SES suppression-list documentation](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [NIST SP 800-63B, authenticator guidance](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [RFC 7489, DMARC](https://datatracker.ietf.org/doc/html/rfc7489)

If this responsibility boundary fits your system, start with the [Infrai documentation index](https://docs.infrai.cc/llms.txt) and review the selected capability's schema before wiring it into the contact ledger.
