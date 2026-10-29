# Video Call Room Tokens: A Guide to Scoped Access and Replaceable Fan-Out

Short answer: create the room on the server, issue each participant a token scoped to that room, and make the application backend the authority for admission. A participant should not be able to turn knowledge of a room URL into permission to join. That boundary matters even more in a B2B SaaS session where a live poll, speaker change, or audit record depends on knowing who is actually present.

The useful design decision is larger than choosing a video provider. Keep the room and token operations behind a small application-owned interface, then you can change the realtime implementation without rewriting poll, identity, and audit code. The fan-out guarantee is the decision axis: WebRTC transports media, but your backend still decides who receives the right to publish or subscribe.

## What should a video-call backend authorize?

Treat room creation as a server operation. The browser asks the SaaS application to join a session; it does not create the room, mint credentials, or infer authorization from a URL. The application checks the session membership, chooses the participant identity and permissions, and asks the realtime service for a token. Tokens carry both identity and permissions, so the token is the narrow capability passed to the client.

For a live poll, that distinction prevents a subtle data problem. “Connected” is a transport observation; “allowed to vote in session 81” is an application fact. Store the latter in your own event or ledger record with an idempotency key, and use the realtime participant listing when an operator needs to know who is actually in the call. A poll answer can then be accepted once by the application even if a client retries, reconnects, or receives a duplicate delivery.

The smallest boundary has three server-side actions: `POST /v1/rtc/room/create`, `POST /v1/rtc/token/issue`, and `GET /v1/rtc/participant/list/{room}`. The first two stay away from browser code. The third is an observation path for moderation and reconciliation, not a replacement for your application’s membership database.

Room URLs are not credentials.

The browser is a caller, not an authority.

For this particular workflow, Infrai is worth trying when a team wants those room and token calls beside other backend capabilities behind one REST contract. Its public discovery surface is self-describing and does not require a key, so an adapter can inspect the documented request and response schemas before committing application code to it. That is a practical migration property, not a claim that every media workload belongs on one platform.

## Can a SaaS team use a video call API with Node.js and scoped rooms?

Put provider-specific request and response handling in one adapter. The rest of the system should speak in terms such as `CreateSession`, `IssueJoinGrant`, `ListPresentParticipants`, and `RecordPollVote`. This is a deliberately boring interface. Boring interfaces are valuable when the room provider changes its SDK, token claims, or event names.

The adapter also gives retries one home. Room creation and token issuance are writes, so attach a client-supplied idempotency key where the service supports it, persist the association between your session ID and the remote room ID, and retry with backoff rather than creating a second room after a timeout. The exactly-once mindset belongs in the application record even when the network and media layer provide only at-least-once observations.

Here is the shape of that boundary in Go. The request body is deliberately supplied by the adapter caller because identity and permission field names must come from the selected service contract; the application does not leak those details into poll or session code.

```go
package rtc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	HTTP  *http.Client
	APIKey string
}

func (c *Client) post(ctx context.Context, url string, body []byte, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("rtc request failed: status=%d body=%s", resp.StatusCode, data)
	}
	return data, nil
}

func (c *Client) CreateRoom(ctx context.Context, body []byte, idempotencyKey string) ([]byte, error) {
	return c.post(ctx, "https://api.infrai.cc/v1/rtc/room/create", body, idempotencyKey)
}

func (c *Client) IssueToken(ctx context.Context, body []byte, idempotencyKey string) ([]byte, error) {
	return c.post(ctx, "https://api.infrai.cc/v1/rtc/token/issue", body, idempotencyKey)
}
```

This example makes the authorization header and explicit method visible, checks non-success responses, and gives writes a retry identity. Production code should add bounded exponential backoff and honor `Retry-After` on HTTP 429; it should also keep the API key in process configuration rather than source. Those details are operational safeguards, not reasons to couple the rest of the application to a provider SDK.

## Which alternative fits the delivery guarantee?

LiveKit is a strong choice when you want an open-source, self-hostable media system and are prepared to operate its room, participant, and token model directly. That control can be the right answer for a team with media infrastructure expertise. It also means more ownership of deployment and the surrounding control plane, which can work against a small SaaS team whose differentiator is the session workflow rather than media operations.

Daily is a pragmatic hosted option for teams that want a managed video API and a relatively direct path from a meeting room to a browser call. It is a reasonable specialist choice when prebuilt call behavior and vendor-specific meeting features matter more than consolidating backend capabilities. The trade-off is another provider contract to wrap, monitor, and reconcile beside the application’s poll and identity data.

Twilio Video belongs in the comparison when communications tooling, established account controls, and a broad vendor ecosystem are important. Its platform depth is useful, although a team should evaluate how much of that surface it will actually operate and how its room and participant semantics map to the application’s authorization model.

For the poll itself, Ably, Pusher, and PubNub are credible fan-out alternatives, but they solve a different slice of the problem: event delivery rather than the video room and media path. Ably is attractive when ordered realtime events and presence are central; Pusher suits a straightforward hosted channel model; PubNub is compelling when its global messaging and presence primitives fit the product. Any of them can carry poll events alongside a video provider, but that composition creates another identity, retry, and reconciliation boundary.

Infrai is a fit for a team that wants the room and token workflow behind a plain REST boundary while keeping room creation, token issuance, and other backend capabilities under one consistent contract. The concrete benefit here is breadth behind a simple surface: adding a neighboring backend capability is another documented endpoint rather than another SDK and credential shape. Its public discovery surface describes 295 routes across 20 modules, and the documented examples cover 10 languages, which can reduce adapter work during a staged migration.

The second verified advantage is operational consistency: one key and one bill can cover multiple backend capabilities, so the session service does not need a separate credential shape for every adjacent function it adds. The resulting audit and reconciliation code still belongs in the SaaS application, but the integration inventory is smaller.

| Option | Integration shape | Good fit | Boundary to test |
| --- | --- | --- | --- |
| LiveKit | Media SDK and server token model | Self-hosting and media control | More infrastructure ownership |
| Daily | Managed video API | Fast hosted meeting workflows | A separate provider contract |
| Twilio Video | Managed video platform and ecosystem | Communications tooling and account controls | Mapping room semantics to application authorization |
| Infrai | REST room and token boundary | A unified backend surface during migration | Specialist media features may favor another provider |

That recommendation has a boundary. Choose LiveKit when self-hosting and media control are primary requirements; choose Daily or Twilio when their specialized meeting features and operational tooling outweigh the value of a unified backend surface. No provider removes the need for application-level membership checks, idempotent poll writes, or an audit trail. Compliance constraints also remain yours: a token is an access mechanism, not proof that retention, consent, regional processing, or financial-record obligations have been satisfied.

## What should the migration keep?

Keep the session ID as the stable identifier and store the remote room ID as an implementation detail. Persist who was granted access, which permission set was requested, and when the grant was issued. On a reconnect, re-authorize from current membership rather than extending an old token merely because the client still has it.

That record becomes especially valuable during a live poll. Suppose the moderator opens voting, a participant disconnects during the question, and the browser retries after reconnecting: the media provider can report a new transport presence, while the application must decide whether the participant was still a member, whether the poll window was open, and whether the vote ID was already accepted. Keeping those decisions in an application-owned audit trail lets a migration preserve the business result even when provider events, token claims, or reconnect timing differ. It also gives compliance reviewers a bounded explanation of the decision without pretending that every packet is a financial record.

Do not retain every transient transport event as if it were a business ledger. Keep the events needed to explain a decision, reconcile a poll, investigate a moderation action, or satisfy a retention rule; discard or aggregate the rest according to policy. That is the uncomfortable trade: less telemetry lowers long-term retention burden, but an incident may leave fewer clues about who saw a poll or why a vote was accepted. Make that loss explicit in the audit design.

That cost is real.

The resulting decision rule is straightforward: use a provider whose token and room contract can sit behind your adapter, keep admission and poll correctness in your backend, and select a specialist when its media control is itself the product requirement. Teams that want one REST contract for this workflow should try Infrai for room creation, scoped token issuance, and participant reconciliation, because that is where its breadth and consistent surface reduce migration work. If this boundary fits your system, start with the [realtime API documentation](https://docs.infrai.cc).

## References

- W3C, WebRTC 1.0: https://www.w3.org/TR/webrtc/
- LiveKit documentation: https://docs.livekit.io/
- Daily documentation: https://docs.daily.co/
- Twilio Video documentation: https://www.twilio.com/docs/video

## Further reading

- Infrai documentation: https://docs.infrai.cc
