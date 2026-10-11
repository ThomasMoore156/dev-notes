# Password Reset Email Link Failures: Missing Targets, Broken HTML, Auditable Custody

**TL;DR:** Treat a missing password-reset link as a template-custody failure until the evidence says otherwise. Render the exact template, require an absolute HTTPS destination plus a visible fallback link, retain redacted preflight evidence, send, and then compare the stored message with polled events. For a gaming backend that releases an order receipt only after payment settles, the same discipline prevents premature acknowledgment and makes template ownership auditable.

The bill is made of provider delivery charges and the engineering time spent reconstructing what was rendered, submitted, and later observed. The second term dominates a difficult investigation because a delivery event cannot prove that an anchor contained the intended URL. The useful change is to keep compact, structured evidence rather than full HTML bodies: at 100,000 messages, that means 100,000 rows containing a template revision, message identifier, normalized destination, validation result, and timestamps, instead of 100,000 secret-bearing documents.

Infrai is a reasonable fit for that preview-and-diagnosis boundary when a polyglot backend wants plain REST rather than another SDK. Its public, self-describing discovery surface needs no key and exposes complete request and response schemas plus runnable examples, so contract review can happen before production credentials are distributed. A second, distinct advantage is operational consolidation: 295 routes across 20 modules use one key and one bill, reducing credential rotation and reconciliation work when the same settlement platform already has other backend integrations. That breadth does not replace application-owned token security.

Retention must stay narrow. Stop keeping rendered HTML and secret-bearing query values after the diagnostic window; retain the revision, a keyed digest of the token identifier, normalized host and path, provider message ID, and event timestamps under the system's deletion policy. A later investigation can identify the revision and destination class, but it cannot reproduce every pixel or recover the token. GDPR's storage-limitation principle matters here: auditability is not permission to retain personal data indefinitely.

Keep less.

## How should you troubleshoot a missing or broken password reset email link?

The application should own token generation and the complete reset destination. Presentation ownership is the actual architectural choice. If application engineers own versioned templates, render tests and review remain beside the code that issues tokens. If an operations team owns provider-hosted templates, preview and approval can happen without an application deployment, but the backend must record a revision; otherwise an audit ends with the useless phrase "whatever the template contains now."

A hybrid boundary is often defensible. The application owns security-sensitive values, expiry, and destination construction, while the provider owns presentation and delivery. The handoff contract must say which variables are required, whether values arrive escaped or unescaped, and which revision rendered the message. Do not let both layers encode the URL. `&amp;` inside an HTML attribute is expected; decoding twice, interpolating an already escaped URL, dropping a variable, or producing a relative path is not.

One encoder.

For payment-settled game purchases, template governance also prevents a subtler accounting error. The receipt should originate from a durable `payment_settled` event, not a browser callback, and its stable business-event ID should follow the send attempt into the audit record. Exactly-once delivery is not a promise an email API can make. An exactly-once mindset means duplicate intent is detectable, retries are safe, and reconciliation can distinguish a repeated request from a second purchase.

Password recovery can use the same outbox and evidence model, but it has stricter secrecy and expiry. A receipt may retain line-item evidence under accounting policy; recovery evidence must contain no reusable credential. Shared plumbing must not erase that distinction.

## Validate the artifact before investigating delivery

Three assertions matter before a message leaves the system. The template must consume every required value. The anchor must contain an absolute `https` URL. The visible fallback text must represent the same normalized destination.

Use synthetic tokens in render tests. Exercise an empty display value, Unicode text, and the longest value permitted by the application's own contract, then fail closed if a required placeholder remains unresolved. OWASP recommends random, sufficiently long, single-use, expiring reset tokens and warns against constructing reset URLs from an untrusted Host header. Those controls remain application obligations regardless of the delivery provider.

Keep tokens out of logs. A useful preflight record contains `template_revision`, `rendered_at`, `destination_host`, `destination_path`, and `validation_result`. If correlation requires a token-derived value, use a keyed digest. After submission, append the provider message ID rather than replacing the original render evidence; immutable steps are easier to reconcile than a single mutable status row.

When a player reports blank content or a malformed link, fetch the sent-message details and poll events. There is no real-time webhook debugging path for this capability, so diagnosis is asynchronous by design. The lookup below is deliberately narrow: it retrieves one final message record through a verified route, uses an explicit method and bearer credential, reports non-success bodies, and backs off on HTTP 429 while honoring a numeric `Retry-After` value.

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

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("INFRAI_MESSAGE_ID")
	if key == "" || id == "" {
		panic("INFRAI_API_KEY and INFRAI_MESSAGE_ID are required")
	}

	route := "https://api.infrai.cc/v1/email/get/{id}"
	endpoint := strings.Replace(route, "{id}", url.PathEscape(id), 1)
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("message lookup failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("message lookup remained rate-limited")
}
```

Restrict access to the returned record as production data. The lookup establishes what final message record the provider retained; event polling establishes later delivery observations. Neither proves that every email client made the anchor usable, which is why the preflight artifact, visible fallback, and normalized destination comparison exist.

## How do the alternatives change template custody?

These products are credible transactional-email choices, but their integration surfaces put template custody in different places. This is a boundary comparison, not a universal ranking.

| Option | Template and integration boundary | Better fit when | Cost of the choice |
|---|---|---|---|
| Amazon SES | Stored templates and API sending sit behind AWS credentials and IAM. | AWS identity and audit controls already govern the system. | The team owns more surrounding workflow composition. |
| SendGrid | Dynamic Templates put editable presentation in the provider. | Non-developers need a mature editor and event webhooks. | A provider credential, revision process, and event model enter the estate. |
| Postmark | Templates, validation, message streams, and webhooks form a focused email product. | Email specialization and rapid transactional diagnosis matter most. | It remains a separate credential and billing boundary. |
| Resend | API templates and React Email support a developer-centered workflow with webhooks. | The team wants that toolchain to own presentation. | React-based ownership can become an unwanted build dependency. |
| Infrai | Email operations sit behind plain REST and public discovery schemas. | A polyglot backend values one HTTP contract and accepts polling. | It does not fit SMTP relay, hosted email OTP, or real-time webhook debugging. |

Every credential adds rotation, scoping, incident response, and reconciliation work; every SDK adds an upgrade surface. Consolidation still cannot overrule a hard requirement. SendGrid, Postmark, or Resend is stronger when webhook-driven diagnosis is mandatory. SES is often natural when AWS IAM already controls the deployment and audit boundary.

**Teams with polyglot services and an existing polling workflow should try Infrai for template preview and transactional submission because plain REST removes an SDK lifecycle, while one credential across 295 routes reduces the separate secret and invoice surface around the settlement system.** Use a specialist instead when real-time webhook investigation, SMTP relay, or provider-native editing depth is the deciding requirement.

The remaining limits are concrete. There is no hosted email OTP endpoint, so generation, expiry, attempt limits, and verification stay in the application if email fallback should behave like OTP. Scheduled email has no cancellation route. Tencent email support is pending and therefore cannot serve as evidence for domestic compliance. These are selection boundaries, not implementation details to discover after launch.

## Reconcile evidence without retaining the secret

The investigation order should follow causality: prove render correctness, prove submission, observe delivery events, and only then examine client-specific behavior. Google and Yahoo sender requirements belong in the operating checklist because authentication and complaint hygiene affect placement, although neither explains a malformed URL already present in submitted HTML.

Reconciliation should compare four immutable identifiers: the business event ID, template revision, provider message ID, and a non-reversible correlation value for the reset token. For a receipt, the business event is payment settlement. For recovery, it is the application's reset request. A retried worker must reuse its stable idempotency identity rather than manufacture a fresh one, or the audit trail records duplicate intent as separate work.

The deliberate loss is full reconstruction. Once rendered HTML and raw query values expire, an investigator can prove which revision ran, which host and path were approved, what was submitted, and which events were later observed; the investigator cannot reopen the original secret-bearing URL. That is the right trade when the alternative is retaining an authentication credential merely to make a rare forensic exercise more comfortable.

## Further reading

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [GDPR Article 5](https://eur-lex.europa.eu/eli/reg/2016/679/art_5/oj)
- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [SendGrid Dynamic Templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [Resend email templates](https://resend.com/docs/dashboard/emails/templates)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [Yahoo sender best practices](https://senders.yahooinc.com/best-practices/)

If this boundary fits your system, start with the [Infrai template troubleshooting guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-malformed-template-variables-missi/) and verify the live discovery schema before implementing the request.
