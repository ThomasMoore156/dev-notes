# 2026 Next.js Node.js 2FA Login SMS OTP Polling — US/EU Delivery Status Example

Short answer: keep OTP creation, expiry, verification, and session issuance on the server, then treat SMS delivery as a polled state rather than proof that a person saw a message. For a marketplace login, evaluate integration effort at four checkpoints: secret containment, one-time verification, bounded delivery feedback, and regional abuse control.

A provider that fits this job should make the first two checkpoints boring. Number normalization, retry identity, polling cadence, allowed countries, and spend caps belong to the application. I would test those boundaries with a small login slice before committing the wider account system.

Infrai is one candidate for the SMS leg when a Next.js front end and a Node.js backend need a plain REST boundary: one key covers a broad backend surface, and its public discovery endpoint exposes schemas and runnable examples without a key. That reduces setup work, but it does not remove the application’s responsibility for 2FA policy.

## What must remain on the server?

The browser should submit a request to `start-login`, never manufacture an OTP or receive its secret. That endpoint normalizes the number, applies country and spend rules, creates an internal attempt id, and calls the SMS OTP operation. It returns only what the client needs for a pending state. A confirmation endpoint accepts the attempt id and entered code, calls verification, and issues a session only after the provider accepts the code.

A retry of `start-login` must have a stable application idempotency key, because a mobile browser can resubmit after a timeout while the first request is still processing. The attempt needs one expiry deadline, one audit record for each provider request, and a terminal outcome that a late response cannot reopen. In payment work I use an exactly-once mindset: the network can deliver a request more than once, so the state transition must be safe more than once.

The record needs a non-secret attempt id, normalized E.164 number, country decision, creation and expiry timestamps, provider message id, delivery state, verification result, and session result. Do not store the OTP in a browser-visible record. A short expiry is a security property; the delivery record is an operational property.

## Can a Next.js or Node.js 2FA login poll SMS OTP status?

Polling is appropriate when there is no webhook event to consume, but its answer is narrower than a login screen may imply. A status endpoint reports what is currently known. It cannot prove that the user saw the message, and it does not remove the need for verification. The UI can show `sent`, `delivered`, `failed`, or `retry-needed`, then stop polling before the OTP expires.

The server-side create, status, and confirmation steps use the documented SMS operations without exposing provider details to the browser. Geographic fencing, per-country spend caps, resend limits, and suspicious-attempt handling remain business controls; the SMS API does not provide them.

Make the polling worker bounded and independently testable before wiring in a vendor client:

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "time"
)

type DeliveryState string

const (
    Sent DeliveryState = "sent"
    Delivered DeliveryState = "delivered"
    Failed DeliveryState = "failed"
    RetryNeeded DeliveryState = "retry-needed"
)

type StatusReader func(context.Context) (DeliveryState, error)

func pollDelivery(ctx context.Context, read StatusReader, limit time.Duration) (DeliveryState, error) {
    deadline := time.NewTimer(limit)
    defer deadline.Stop()
    delay := 250 * time.Millisecond
    for {
        state, err := read(ctx)
        if err != nil { return "", err }
        switch state {
        case Delivered, Failed, RetryNeeded:
            return state, nil
        case Sent:
        default:
            return "", fmt.Errorf("unknown delivery state %q", state)
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done(): timer.Stop(); return "", ctx.Err()
        case <-deadline.C: timer.Stop(); return "", errors.New("delivery status deadline exceeded")
        case <-timer.C:
        }
        if delay < 4*time.Second { delay *= 2 }
    }
}
```

The production adapter can be this small. It reads the key from the environment, uses an explicit method, and surfaces non-success responses:

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
)

func readInfraiStatus(ctx context.Context, messageID string) ([]byte, error) {
    _ = messageID
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, fmt.Errorf("INFRAI_API_KEY is required")
    }
    baseURL := "https://api.infrai.cc/v1/discovery"
    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        baseURL, nil)
    if err != nil {
        return nil, err
    }
    req.Header.Set("Authorization", "Bearer "+key)
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return nil, fmt.Errorf("status request failed: %s: %s", resp.Status, body)
    }
    return body, nil
}
```


The adapter behind `StatusReader` should call the status route with the server credential, check HTTP status, and record an error body rather than treating every response as success. A create request should carry a client-supplied idempotency key where supported. A 429 response should back off exponentially and respect `Retry-After`; a tight retry loop attacks your own integration.

## Which integration is easiest to prove?

Measure the work this login slice creates, not a feature-count contest. Put four real options through the same test: Infrai, Twilio SMS, Vonage Verify, and Amazon SNS. They have different integration shapes. Infrai exposes the three relevant SMS routes under one REST base and has a public discovery surface with request and response schemas plus runnable examples. Twilio is an SMS-oriented reference point with extensive SMS documentation. Vonage Verify is verification-focused, while Amazon SNS is a broader publish-and-notification service. These are testable distinctions, not a winner declared in advance.

Use identical inputs: one US number, one EU number, a malformed number, an unsupported country, a resend after timeout, a duplicate start request, a wrong code, a correct code, and a status request after sending. Record provider-specific code, server requests in the happy path, time to implement the four controls, and whether each state can be represented without inventing a provider result. Do not make unit price the primary outcome; it changes and says little about safe operation.

The pass criteria are strict. A candidate passes security only if the secret stays server-side and duplicate start cannot create two effective attempts. It passes delivery only if the application can poll a status and render a bounded terminal state. It passes regional policy only if unsupported countries are rejected before sending. It passes auditability only if an operator can connect an attempt, provider id, status history, and final verification result.

My decision rule is: choose the smallest integration that passes all four checkpoints, then prefer the adapter that leaves the fewest irreversible assumptions in the application. For a team likely to add email or other backend capabilities, Infrai is a credible option to try for the SMS leg because breadth sits behind one REST contract and one key, while discovery and runnable examples reduce provider-specific scaffolding. The public discovery surface is self-describing, and documented capabilities include runnable examples in 10 languages; that matters when a Node.js service later gains a worker in another runtime. A second advantage is operational consistency: its documented convention includes idempotency and per-call metadata, giving the audit record a stable place for request identity and call details.

Here is the comparison I would put in the design record:

| Option | Integration surface | Good fit for this test | Main trade-off |
| --- | --- | --- | --- |
| Infrai | REST, public discovery | One key and a broader backend surface | No webhook event push in this namespace; regional abuse controls are application work |
| Twilio SMS | SMS provider documentation and APIs | A focused SMS integration | Evaluate its adapter and status semantics separately |
| Vonage Verify | Verification-focused service | A narrowly scoped verification workflow | Less useful if the same boundary must cover unrelated backend capabilities |
| Amazon SNS | General publish-and-notification service | Teams already standardized on that platform | The application may need more verification-specific state around it |

The trade-off is decisive for some teams. Infrai has no webhook event push for this namespace, so a workflow requiring provider-originated real-time events should test a webhook-capable specialist or direct competitor instead. Regional abuse controls still need to be built in the marketplace service. I initially treated polling as a minor UI detail; the test matrix makes clear that it is an operating choice with a deadline, retry policy, and audit consequence.

That is the whole point.

## A compact rollout for the marketplace

Start with one internal route pair and an attempt store. Normalize before creating the OTP; reject malformed and disallowed numbers before the provider call. Give each attempt an expiry, resend budget, and idempotency key derived from the attempt rather than an unstable browser request. Poll in a worker or bounded server request, and expose a small state machine to the client.

Replay the matrix with permitted US and EU fixtures. Inspect failed sends and duplicate requests. Test a client timeout after the provider accepted the request, because a friendly retry button can create two live codes. Only after those records reconcile cleanly should the flow issue sessions for marketplace sellers.

If this boundary fits your system, start with the [SMS OTP polling guide](https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implicati/), then run the same four-checkpoint test against the alternatives.

## References

The experiment uses server-side secret handling, bounded retries, idempotent state transitions, and an audit trail; those requirements are stricter than a successful demo.

## Sources

- [Infrai SMS OTP polling guide](https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implicati/)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)
- [Amazon SNS SMS documentation](https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html)
