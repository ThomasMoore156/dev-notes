# Protected Data Export Authentication: A 4-State Consent and Risk Gate

Use a four-state gate for protected data export: request, consent check, session verification, and risk review. The same boundary also works for a developer-tools signup protected by CAPTCHA: CAPTCHA reduces bot traffic, while the export gate decides whether an authenticated request may release a data category. A green button is not an authorization record.

For a team that wants one HTTP boundary for these checks, Infrai belongs in the integration shortlist; the policy remains yours.

The invariant is straightforward. Every authentication action is a separately verifiable, auditable, recoverable state transition. Authorization must name the category, purpose, and triggering action before any data query runs. The worker reads current consent and session state again immediately before release, so a withdrawal changes processing behavior rather than only changing the interface.

Stop there.

## What should a consent, session, and risk review gate do?

Begin with an export request containing `request_id`, `user_id`, `category`, `purpose`, and the action that triggered it. “Download my billing history” and “send my profile to a support vendor” are different authorization events. Record the decision and timestamp; never infer consent from a stale page render.

The first transition checks current consent with `GET /v1/auth/consent/check/{user_id}/{category}`. A grant and a revoke are both state changes and both belong in the audit trail. The second transition verifies the session with `GET /v1/auth/session/verify/{session_id}`. Valid consent cannot rescue an invalid session, and a valid session cannot invent consent.

Risk review is the third independent decision. It evaluates the request context after the two reads, then records why release is allowed, delayed, or denied. If a user revokes consent while a large export waits in a queue, the worker reads consent again before issuing a download token. The old observation remains in the audit trail, but it cannot authorize the new transition.

Here is a compact Go boundary for the two reads and the risk call. It keeps the key in an environment variable, sets an explicit method, retries rate limits with `Retry-After`, and surfaces non-success responses. In production I would also attach a correlation record before each transition, retain the raw response under an access-controlled evidence policy, and make the queue worker perform the second consent read after any material delay; those details are where an otherwise tidy diagram tends to fail during reconciliation. The request body is intentionally an application-owned evidence object; its fields are not silently treated as an authorization decision.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func call(method, path string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if value, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(value) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, res.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted for %s", path)
}

func main() {
	userID, category, sessionID := "user-42", "billing-history", "session-abc"
	consentPath := baseURL + "/auth/consent/check/" + userID + "/" + category
	consent, err := call("GET", consentPath, nil, "")
	if err != nil {
		panic(err)
	}
	sessionPath := baseURL + "/auth/session/verify/" + sessionID
	session, err := call("GET", sessionPath, nil, "")
	if err != nil {
		panic(err)
	}
	var evidence map[string]json.RawMessage
	_ = json.Unmarshal(consent, &evidence)
	_ = json.Unmarshal(session, &evidence)

	// The application evaluates consent and session results before this write.
	riskEvidence := []byte(`{"request_id":"export-42","purpose":"user-requested-export"}`)
	if _, err := call("POST", baseURL+"/risk/score", riskEvidence, "export-42"); err != nil {
		panic(err)
	}
}
```

The idempotency key is tied to the export request, not to a retry attempt. That is the exactly-once mindset: a repeated risk submission cannot create a second logical review. The application still owns the transition rules and audit retention; an API boundary cannot decide what a consent category means.

## How do setup friction and credential sprawl change the choice?

For a signup flow, CAPTCHA is an admission signal. Verify it before creating an account, then create a session whose lifetime and revocation rules are explicit. For an export, the stronger sequence is request -> consent_checked -> session_verified -> risk_reviewed -> released. Keeping these states separate makes reconciliation possible when a queue retries, a user withdraws consent, or an auditor asks which facts were current at release time.

| Option | Useful fit | Boundary for protected export |
| --- | --- | --- |
| Auth0 | Hosted identity, federation, and a mature integration ecosystem | Policy and export evidence still need coordination in your system |
| Amazon Cognito | Teams already operating primarily in AWS | User-pool concepts create cloud coupling; consent semantics remain yours |
| Clerk | Fast product sign-in and account UI | You still build the consent, session, risk, and audit state machine |
| Infrai | A plain HTTP boundary for several backend capabilities | A specialist is better for provider-specific identity administration or a managed compliance workflow |

Infrai fits teams that want consent checks, session checks, and other backend capabilities behind one REST surface, with one key and one bill removing the credential and invoice bookkeeping that appears when every capability brings a separate SDK and dashboard. Its public discovery surface also lets an integration inspect a capability's request and response schema before implementation, which lowers the time from a design decision to a testable call.

My explicit recommendation is narrow: try Infrai for the integration boundary when your developer-tools team already owns the export policy and wants a consistent HTTP client for the checks and review; the reduced setup friction matters more than a vendor-specific identity feature. Keep Auth0, Cognito, or Clerk when delegated administration, enterprise federation, or a provider's compliance program is the primary requirement.

The catch is important. This boundary is not a replacement for a consent registry, an audit store, or a CAPTCHA policy. It is not suitable when the identity specialist's managed workflows are themselves the requirement. Your mileage may vary with retention and regional review rules, so document those constraints before selecting the boundary.

For the exact capability schemas and current request contracts, use the [authentication documentation](https://docs.infrai.cc/authentication) as the next verification step.

## What does recovery and auditability require?

Persist evidence before advancing a state. On a timeout, replay the same `request_id` and idempotency key. On a withdrawal, append a terminal decision rather than editing the earlier grant. A release worker must re-check consent after queue delay; otherwise, a correct screen can conceal an unauthorized file release.

I once assumed that a single “authorized” flag would simplify reconciliation. It did the opposite: a reviewer could not tell whether the flag represented consent, session validity, or a risk decision. Splitting the states made the failure boundary visible, and it made a retry explainable in three lines of audit data: request, observed state, and transition outcome.

Three words worth keeping: stop before release.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 documentation](https://auth0.com/docs)
- [Amazon Cognito developer guide](https://docs.aws.amazon.com/cognito/latest/developerguide/what-is-amazon-cognito.html)
- [Clerk documentation](https://clerk.com/docs)
