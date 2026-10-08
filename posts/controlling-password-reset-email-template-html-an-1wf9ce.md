# Controlling Password Reset Email Template HTML and Text Through Release Contracts

TL;DR: The identity service should own the meaning, authorization, and audit record of a password reset, while a versioned message package should own its HTML, plain-text alternative, subject, and brand rules. The delivery adapter may transport the rendered message, but it should neither invent recovery copy nor receive a reusable reset credential. For a logistics platform that also sends signup verification links, this boundary prevents two superficially similar emails from collapsing into one ambiguous template and gives reviewers a stable artifact to preview before release.

A reset email is part of an authentication ceremony, not a marketing surface. Its design has to preserve that distinction through rendering, accessibility checks, dark-mode behavior, retries, and provider changes. The short link-shaped path is where most of the risk sits: the message must identify the requested action without exposing account state, the token must be single-purpose and time-limited, and every send attempt must be traceable without logging the secret itself.

## How should a password reset email template coordinate HTML and text?

Template ownership belongs with the team that owns the identity contract, expressed as a reviewed and versioned package rather than mutable delivery-console content. This is an organizational choice with architectural consequences. If a logistics communications team owns every email because it owns dispatch notices, then recovery wording, expiry claims, and link construction can change outside the authentication release process. If the identity service embeds an unreviewable HTML string, brand and accessibility specialists cannot safely contribute. The useful middle ground is a template package whose schema and security copy are controlled by identity engineering, while designated brand and accessibility reviewers approve changes through source control.

Keep signup verification and password recovery as separate message types even when their visual shell is shared. A verification link proves control of an address during enrollment; a recovery link authorizes a route into an existing account. They need distinct purpose values, event names, rate limits, and audit entries. Reusing a layout is fine. Reusing semantics is not.

The system of record should retain an append-only event such as `recovery_message_requested`, followed by rendering and handoff outcomes keyed by an internal operation ID. Store a hash or identifier for the token, never the bearer value. NIST SP 800-63B treats recovery and reset as security-sensitive authenticator lifecycle operations; that is a stronger constraint than visual consistency. The audit trail must answer which template version rendered, which locale was selected, when the credential expired, and whether a delivery system accepted the handoff, without making the audit store another credential leak.

This is the trade-off.

## Derive the message contract before writing HTML

Start with immutable input data. The renderer needs less information than the account service usually holds: a display-safe organization name, a preconstructed HTTPS action URL, an expiry description derived from policy, a support URL, locale, and template version. It does not need a password hash, full profile, shipment history, or raw token as a separate field. Passing only the complete action URL also keeps token concatenation out of the template layer.

A practical contract has three outputs: subject, HTML, and plain text. Both bodies must communicate the same action and expiry. The plain-text part is not a dump of tags; it is a deliberately authored alternative with an explicit URL. The HTML part should use a real anchor with descriptive link text, preserve readable contrast, survive blocked images, and remain understandable without color. A heading that says "Reset your password" and a link that says "Reset password" are clearer than "Click here."

Dark mode changes colors differently among mail clients, so the contract should define resilient behavior rather than promise pixel identity. Declare supported light and dark color schemes, provide explicit dark-mode overrides where clients honor them, and choose foreground/background pairs that remain distinguishable when a client transforms colors. Logos need transparent padding or an alternate asset that remains legible against both backgrounds. Critical meaning cannot live in the logo.

The copy should also avoid revealing whether an address is registered. The request endpoint can return the same public response for known and unknown addresses, while only the known-account branch creates a credential and queues a message. That protects the account directory, but internal events still need different, access-controlled outcomes so operators can reconcile abuse controls and delivery work.

## A focused renderer with deterministic preview output

The example uses Go because a small typed renderer makes ownership boundaries visible. It renders both alternatives from the same validated input, escapes display values, and returns bytes that a preview endpoint or test can inspect. Token issuance remains outside the renderer.

```go
package recoverymail

import (
    "bytes"
    "fmt"
    "html/template"
    "net/url"
    "strings"
    texttemplate "text/template"
)

type Input struct {
    Organization string
    ActionURL    string
    ExpiresIn    string
    SupportURL   string
}

type Message struct {
    Subject string
    HTML    []byte
    Text    []byte
}

const htmlBody = `<!doctype html>
<html lang="en"><head>
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<style>
body { margin: 0; background: #ffffff; color: #171717; font: 16px/1.5 Arial, sans-serif; }
main { max-width: 600px; margin: 0 auto; padding: 32px 20px; }
a.action { display: inline-block; padding: 12px 18px; background: #075985; color: #ffffff; }
@media (prefers-color-scheme: dark) {
  body { background: #111827; color: #f9fafb; }
  a.action { background: #7dd3fc; color: #082f49; }
}
</style></head><body><main>
<h1>Reset your password</h1>
<p>We received a request to reset the password for your {{.Organization}} account.</p>
<p><a class="action" href="{{.ActionURL}}">Reset password</a></p>
<p>This link expires {{.ExpiresIn}}. If you did not request it, you can ignore this email.</p>
<p>For help, visit <a href="{{.SupportURL}}">account support</a>.</p>
</main></body></html>`

const textBody = `Reset your password

We received a request to reset the password for your {{.Organization}} account.

Reset password: {{.ActionURL}}

This link expires {{.ExpiresIn}}. If you did not request it, you can ignore this email.
Support: {{.SupportURL}}
`

func Render(in Input) (Message, error) {
    if strings.TrimSpace(in.Organization) == "" {
        return Message{}, fmt.Errorf("organization is required")
    }
    for name, raw := range map[string]string{"action URL": in.ActionURL, "support URL": in.SupportURL} {
        parsed, err := url.ParseRequestURI(raw)
        if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
            return Message{}, fmt.Errorf("%s must be an absolute HTTPS URL", name)
        }
    }
    h, err := template.New("recovery.html").Parse(htmlBody)
    if err != nil { return Message{}, fmt.Errorf("parse HTML template: %w", err) }
    t, err := texttemplate.New("recovery.txt").Parse(textBody)
    if err != nil { return Message{}, fmt.Errorf("parse text template: %w", err) }

    var htmlOut, textOut bytes.Buffer
    if err := h.Execute(&htmlOut, in); err != nil { return Message{}, fmt.Errorf("render HTML: %w", err) }
    if err := t.Execute(&textOut, in); err != nil { return Message{}, fmt.Errorf("render text: %w", err) }
    return Message{Subject: "Reset your password", HTML: htmlOut.Bytes(), Text: textOut.Bytes()}, nil
}
```

A preview API can call `Render` with fixture data and return the three outputs plus the template version. It must never mint a live credential. Protect the route with normal engineering access controls, reject arbitrary remote asset URLs, and mark preview responses non-cacheable if they can contain internal brand material. For pull requests, deterministic HTML and text snapshots make review straightforward; rendered screenshots may supplement those artifacts, but they should not replace semantic assertions.

The renderer is intentionally boring. That helps. Given identical inputs and a pinned template version, it returns identical content, which makes approval records and incident reconstruction defensible.

## Preview checks should behave like transaction checks

A visual preview catches clipping and broken layout, yet it cannot establish that the recovery workflow is correct. The test suite should parse the HTML and assert one intended recovery link, an HTTPS scheme, meaningful anchor text, a language declaration, and the absence of the raw token in logs or telemetry fixtures. It should compare the action URL and expiry statement across HTML and text. It should also render unusually long organization names, non-ASCII names, and an expired fixture so escaping and layout failures become release failures rather than user reports.

Treat accessibility as inspectable properties. WCAG 2.2 defines a minimum contrast ratio of 4.5:1 for normal text and 3:1 for large text at Level AA, subject to its definitions and exceptions. Those thresholds belong in automated color tests for declared theme pairs. Keyboard and screen-reader review still matters because a numeric contrast test cannot decide whether link wording makes sense out of context. Run the message through representative desktop, mobile, webmail, light-mode, and dark-mode clients; document the tested matrix and date because client behavior changes.

Delivery is an at-least-once environment even when the account operation must behave exactly once. Assign one idempotency key to the logical recovery request and persist the state transition before dispatch. A retry may repeat a provider call after an uncertain timeout, so reconcile by operation ID and accept that duplicate mail can occur; the authentication service, however, should enforce the credential's single-use transition atomically. **Exactly-once belongs to credential consumption, not to the network.**

Google's sender guidance adds transport obligations for mail sent to personal Gmail accounts, including authentication expectations and, for bulk senders, additional requirements. Those controls help delivery and abuse resistance, but they do not repair an inaccessible or misleading template. Monitor authentication results, bounces, complaints, queue age, and acceptance separately. Acceptance is not inbox placement, and neither proves that a user could complete recovery.

## Compare ownership models after defining the invariants

Once the constraints are explicit, the ownership decision becomes less subjective.

| Model | Change control | Preview fidelity | Security boundary | Best fit |
|---|---|---|---|---|
| Identity-owned versioned package | Coupled to reviewed identity releases | Deterministic from source | Secrets and semantics stay near issuance | Recovery and verification messages with strict audit needs |
| Central communications repository | Shared review across message types | Consistent if render dependencies are pinned | Requires an explicit identity contract | Organizations with a staffed messaging platform team |
| Delivery-system hosted template | Changes may occur outside application releases | Closest to final delivery rendering | Variables become part of the auth contract | Low-risk messages or tightly governed hosted content |

No row removes the need for approvals, rollback, and evidence. The deciding question is where a mistaken edit can violate an authentication invariant and who can restore a known version under pressure. For recovery mail, source-controlled ownership near identity usually reduces the number of independent state stores involved, while a central repository can work when its release policy gives identity owners mandatory review. Hosted templates demand careful version pinning and exportability because the deployed artifact otherwise drifts from the code that claims to describe it.

Provider independence has a cost.

A source-controlled renderer is a poor fit when a small team cannot maintain client-test fixtures, accessibility review, and deployment automation; under that limitation, a tightly governed hosted template can be the more defensible choice, provided versions are immutable, exports are retained, and identity owners approve security copy. A central repository also introduces a release dependency: it can improve consistency across shipment, signup, and recovery mail, but an unavailable communications pipeline may delay an urgent identity change. Conversely, embedding every asset and sentence in the identity binary reduces external dependencies while making routine brand updates wait for an application deployment. There is no ownership model that maximizes independent release speed, preview fidelity, and minimal operational surface at once. Choose which failure is easiest to detect and reverse, then record that decision beside the template version.

This is where the logistics context matters. Shipment alerts may tolerate copy experiments and frequent operational edits; account verification and recovery should not inherit that cadence merely because all three travel through email. Separate policy domains can share rendering infrastructure while retaining different approvers and deployment gates.

## Roll out without changing recovery semantics

Begin by recording the current template checksum, inputs, and observable workflow. Add the new renderer in shadow mode with synthetic URLs, compare normalized HTML and plain text, then approve the version against the accessibility and client matrix. Deploy to a small internal cohort before increasing exposure, but keep credential creation and consumption unchanged throughout the template migration.

Rollback should select the preceding signed template version, not reconstruct content manually in a delivery console. Reconcile counts from accepted recovery requests through credential creation, render, handoff, and successful consumption using the internal operation ID. Do not place the action URL in metrics. A compact rollout is successful when the new presentation can move independently, while the security semantics, idempotency rule, and audit chain remain fixed.

## Sources and References

- https://support.google.com/a/answer/81126
- https://pages.nist.gov/800-63-3/sp800-63b.html
- https://www.w3.org/TR/WCAG22/
- https://www.rfc-editor.org/rfc/rfc5322
- https://www.rfc-editor.org/rfc/rfc8058
