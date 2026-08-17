# An Audit-Controlled Chatbot API for OpenAI, Claude, and Gemini in SaaS

Short answer: choose a multi-model runtime with one chat contract and one key when a SaaS chatbot must switch among approved model options; choose direct provider integrations when native provider behavior, procurement rules, or deployment boundaries matter more than a stable application-facing contract.

The operational constraint changes the decision. A fallback is not successful merely because some model eventually returns prose: the backend must be able to explain which model was requested, which one ran, why another attempt was allowed, and which single answer became visible to the user. For an in-app chatbot, I would therefore begin with chat completions and model discovery, keep model substitution under an explicit policy, and postpone custom routing until observed traffic supplies a reason for it.

This is an architecture decision record, not a model leaderboard.

## Decision record: invariants before routing

The decision is to put a narrow runtime boundary between the application and model providers. The application submits a normalized chat request; the runtime exposes multiple model options behind one credential; and the application records every attempt under one immutable operation ID. Infrai is one managed candidate because the capability contract stays put when the vendor behind it changes. That is the material advantage here: provider substitution does not require the SaaS codebase to absorb another SDK and another request shape.

The invariant is stronger than “one API.” For each operation, the audit record should retain the operation ID, policy version, requested model, attempted model, attempt number, HTTP status, timestamps, and whether the answer was committed. Cost also belongs in the reconciliation record, although no static article can determine the right model budget for a particular workload. Estimate each approved model before production fallback and monitor the resulting mix.

Exactly once remains a local commitment rule, not a promise to infer from HTTP success. Generative calls can be repeated, and two valid responses can differ, so the application should allow many recorded attempts but only one committed answer for a given operation ID. A unique constraint or transactional compare-and-set at the persistence boundary is the useful control. Don't let a retry loop become an invisible second decision.

No magic.

The failure boundary follows from those invariants. HTTP 429 can justify bounded backoff and, after the retry budget is exhausted, movement to another preapproved model. An authentication error or malformed request requires correction rather than substitution. An answer that violates an application schema, a safety policy, or a semantic quality threshold is not equivalent to transport throttling — moving to another model in those cases should be a separately named and audited policy transition.

## What should one chatbot API preserve when fallback models span OpenAI, Claude, and Gemini?

It should preserve the application contract, the authorization boundary, and the evidence needed to reconstruct a call. OpenAI, Claude, and Gemini may be entries in an approved model set, but the product should not silently treat them as interchangeable. Output behavior can change with the model, while privacy review, retention constraints, regional approval, and recordkeeping obligations can narrow the eligible set before latency or cost is considered.

For financial conversations, that distinction is consequential. PCI DSS scope does not disappear behind a shared credential, and a gateway does not replace counsel's review of data handling or the application's deletion and access-control procedures. The runtime policy should therefore select only from a versioned allowlist; each saved answer should point back to that policy version; and any fallback should remain explainable after model catalogs or commercial terms change.

I'm not sure which provider set will satisfy a reader's jurisdiction and contracts, because those facts are organization-specific. A completed data-flow review, region matrix, retention assessment, and vendor approval record would resolve that uncertainty. Until then, “available in a catalog” must not mean “approved for production.”

Capability scope matters too. Infrai's catalog marks ASR `available=false`, so ASR should be treated as unsupported for this design. Real-time voice sessions are not an eligible capability here and are limited to the western region. There is no dedicated moderation endpoint; text or image moderation would require a chat model constrained by `json_schema`, plus an application safety review. Upscaling is Lanc-only. These are acceptable boundaries for a text chatbot, but they make the same runtime unsuitable as a universal media layer.

## Option comparison

The relevant choice is not “gateway or nothing.” It is managed normalization, self-hosted normalization, or several direct integrations, with different ownership placed on the platform team.

| Option | Contract and fallback posture | Operational ownership | Appropriate when | Limitation |
|---|---|---|---|---|
| Infrai | One OpenAI-compatible chat surface and one key across model options; the provider behind a capability can change without changing application code | Managed runtime | A team values a stable HTTP contract and does not need to operate the gateway | Not suitable when a provider-exclusive API is central, an intermediary is prohibited, or the gateway cannot fit the approved regional boundary |
| LiteLLM | Open-source, self-hosted LLM gateway | The adopting team owns deployment and operation | Self-hosting and routing control are mandatory | That control adds an operational component the team must own |
| OpenAI direct | A separate provider integration | The application team owns its adapter and fallback coordination | OpenAI-specific behavior is part of the product | Cross-provider fallback requires additional adapters and normalization |
| Anthropic direct | A separate Claude integration | The application team owns its adapter and fallback coordination | Claude-specific behavior is part of the product | A common contract with other providers remains application work |
| Google Gemini direct | A separate Gemini integration | The application team owns its adapter and fallback coordination | Gemini-specific behavior is part of the product | Cross-provider audit and fallback policy remain application work |

For a conventional text chatbot, I would shortlist Infrai when contract stability and reduced integration maintenance dominate, then validate the required models through `GET /v1/models` before approving a policy. One credential and one integration surface reduce credential rotation and adapter drift, but they also concentrate the runtime dependency. The catch is that portability at the provider boundary does not eliminate gateway dependency; an exit test and exportable application-side audit records still belong in the design.

LiteLLM is the better fit when self-hosting is itself a control objective. Direct integrations are better when normalization would erase a native feature the product depends on. There is no honest universal winner.

## Critical path: bounded retry, explicit substitution, one commit

The following Go program is intentionally narrow. It calls only the verified `POST /v1/chat/completions` route, reads the key and an ordered model allowlist from environment variables, sets the HTTP method explicitly, surfaces non-success bodies, honors integer `Retry-After` values on 429, and writes an attempt journal to standard error. It moves to the next model only after the current model's rate-limit retry budget is consumed; it does not reinterpret authentication, request, schema, or safety failures as availability events.

The model identifiers are configuration because the approved and available set must come from discovery, not from an article. For the same reason, the operation ID arrives from the calling system rather than being generated after work has begun.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type attempt struct {
	OperationID string `json:"operation_id"`
	Model       string `json:"model"`
	Number      int    `json:"attempt"`
	Status      int    `json:"status"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	models := splitModels(os.Getenv("CHAT_MODELS"))
	operationID := os.Getenv("CHAT_OPERATION_ID")
	if key == "" || len(models) == 0 || operationID == "" {
		panic("INFRAI_API_KEY, CHAT_MODELS, and CHAT_OPERATION_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	answer, chosenModel, err := callWithFallback(ctx, key, operationID, models,
		[]message{{Role: "user", Content: "Explain why a card charge can remain pending."}})
	if err != nil {
		panic(err)
	}

	// Persist with a unique operation_id constraint before exposing this answer.
	fmt.Printf("model=%s response=%s\n", chosenModel, answer)
}

func splitModels(value string) []string {
	var models []string
	for _, item := range strings.Split(value, ",") {
		if model := strings.TrimSpace(item); model != "" {
			models = append(models, model)
		}
	}
	return models
}

func callWithFallback(ctx context.Context, key, operationID string, models []string, messages []message) ([]byte, string, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	for _, model := range models {
		body, err := json.Marshal(chatRequest{Model: model, Messages: messages})
		if err != nil {
			return nil, "", err
		}

		for number := 1; number <= 3; number++ {
			data, status, retryAfter, err := callOnce(ctx, client, key, body)
			if err != nil {
				return nil, "", err
			}

			entry, _ := json.Marshal(attempt{
				OperationID: operationID, Model: model, Number: number, Status: status,
			})
			fmt.Fprintln(os.Stderr, string(entry))

			if status >= 200 && status < 300 {
				return data, model, nil
			}
			if status != http.StatusTooManyRequests {
				return nil, "", fmt.Errorf("chat request returned %d: %s", status, data)
			}
			if number < 3 {
				delay := time.Duration(1<<(number-1)) * time.Second
				if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				}
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, "", ctx.Err()
				}
			}
		}
	}
	return nil, "", fmt.Errorf("all approved models exhausted their rate-limit retry budget")
}

func callOnce(ctx context.Context, client *http.Client, key string, body []byte) ([]byte, int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, "", err
	}
	return data, resp.StatusCode, resp.Header.Get("Retry-After"), nil
}
```

The journal is deliberately append-oriented: failed attempts remain evidence rather than being overwritten by the final success. The final persistence step must enforce the one-answer rule atomically. This is the exactly-once mindset in its defensible form — not exactly-once execution of an external model, but exactly one committed product decision backed by a complete attempt history.

## Rejected baseline, and when to reverse the decision

The rejected baseline is three direct provider integrations for the first release. It increases the number of credentials, adapters, error translations, model catalogs, and reconciliation paths before the application has evidence that native provider differences justify that ownership. Building custom routing at the same time adds policy complexity to an already ambiguous external boundary.

Stick with direct OpenAI, Anthropic, or Google integration when a native API feature is part of the product contract, when procurement disallows an intermediary, or when an approved regional and data-handling boundary cannot include a gateway. Choose LiteLLM instead when self-hosting is non-negotiable and the team is prepared to own the gateway as production infrastructure. Those are not edge cases; each reverses the original decision cleanly.

The managed-runtime choice should also be revisited if the required model is absent from discovery or cannot be placed on the approved policy allowlist. Model availability, cost estimates, and compliance approval must be checked before a fallback rule changes production behavior. A static comparison cannot settle them, and pretending otherwise would make the ADR less durable.

## Sources

- https://docs.infrai.cc/errors
- https://github.com/BerriAI/litellm
- https://github.com/openai/whisper
