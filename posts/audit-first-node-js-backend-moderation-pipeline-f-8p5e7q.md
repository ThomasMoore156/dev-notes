# Audit-First Node.js Backend Moderation Pipeline for User Content

**Short answer:** For financial user-generated content, make the moderation result a portable, versioned JSON decision and make the review queue part of the ledger-like state machine, rather than binding publication to a provider-specific response shape.

That choice changes what “simple” means. A single model request can be simple; an auditable publishing decision is not. The backend must preserve the content revision, policy version, decision evidence, and handoff state so that a retry, an appeal, or a provider migration does not rewrite history.

## The ledger constraint comes before the classifier

Treat an incoming post, payment note, or support message as an event with a stable revision ID. The client supplies content, but the server selects the policy version and owns the transition from `received` to `allowed`, `blocked`, or `review`. A model is evidence in that transition. It is not the authority that silently changes a financial record.

The minimum decision record is deliberately boring: tenant ID, content ID, revision digest, policy version, model identifier, decision, confidence, reasons, request ID, and timestamps. The digest distinguishes an edited message from the earlier bytes; the policy version explains which definitions were applied; and the request ID joins the application record to transport logs. Keep the raw text under the retention and access rules that apply to its classification, rather than assuming a digest alone satisfies every audit or appeal requirement. I'm not sure one retention period can be defended across jurisdictions, products, and data classes, so that decision belongs with compliance and policy owners.

The review queue should hold a reference to this immutable decision, not a second interpretation of the content. A reviewer can append a human disposition with actor, reason, and time. Do not overwrite the model result: the disagreement is part of the audit trail, especially when a policy release later changes the outcome for the same category.

Short rule: abstain on uncertainty.

Exactly-once delivery is a useful mindset, not a property to assume from an HTTP call. A client may retry after a timeout, and a worker may receive the same queue item twice. Derive an idempotency key from the content revision and policy version, enforce uniqueness in the database, and use a transactional outbox for the queue handoff. The inference request may run twice; publication and review-task creation must not.

## What should a Node.js backend preserve when moderating financial user content?

The application boundary should have four explicit stages: authenticate and bound the input, load a versioned policy, call a model through an adapter, then validate and persist the result. JSON output is useful because it gives the adapter a contract, but it is not validation. The server must reject unknown decision values, missing reasons, invalid confidence ranges, duplicate records, and responses that cannot be parsed. A parse failure becomes `review` or a retriable indeterminate state; it never becomes `allow`.

For this scenario, the contract can be small:

```go
type ModerationDecision struct {
	Decision   string   `json:"decision"`
	Confidence float64  `json:"confidence"`
	Reasons    []string `json:"reasons"`
}
```

The threshold is also application policy. Confidence is routing evidence, not automatically a calibrated probability. Evaluate false allows and false blocks separately against a labeled sample of the product's own languages and financial vocabulary. A threshold that looks reasonable in a general corpus can be reckless for payment references, account identifiers, or fraud reports whose meaning depends on local context.

If the model call is synchronous, give the publishing path a bounded deadline and define the visible state for a timeout. If it is asynchronous, reserve the content until a terminal decision exists. Both designs can use the same state transitions and idempotency key; that is the portability worth protecting.

## Make provider portability a measured contract

Portability is not achieved by swapping a base URL. It is achieved when the application depends on a narrow interface and tests the semantics behind it. The adapter should expose classification, not raw provider objects, and it should normalize transport errors, structured output, request IDs, and retry behavior into application-owned types.

I use a small compatibility matrix before changing a model path: supported input types, JSON-schema behavior, maximum context, timeout behavior, rate-limit signaling, data-handling controls, and the evidence needed for an audit. A `429` is not equivalent to an invalid JSON response, and neither is equivalent to a policy rejection. Mixing them produces misleading operational metrics and makes a provider migration look safer than it is. The request deadline in the example is 10 seconds; tune it from observed queue and payment-path SLOs, not from a vendor slogan.

No silent fallback.

| Failure or decision | Durable record | Next action |
|---|---|---|
| Valid `allow` or `block` | One decision keyed by content revision and policy | Continue the permitted state transition |
| Valid `review` | One pending decision and one outbox event | Assign a human task without publishing |
| Timeout, `429`, or malformed JSON | Indeterminate attempt with request metadata | Retry within bounds or route to review |
| Duplicate delivery | Existing idempotency key | Return the recorded outcome; do not enqueue twice |

This table is the operational contract I want in a runbook (the audit record stays append-only), not a promise about any particular model.

The trade-off is real. A narrow interface protects the ledger and queue, but it can hide capabilities that matter to a regulated workflow, such as a provider-specific safety control or a required input modality. Stick with a direct integration when that native control is a mandatory requirement. Use an open gateway or a self-hosted gateway when the team can own its compatibility tests, upgrades, secrets, and operational burden. Your mileage may vary because portability depends on the workload and on the controls the compliance review actually requires.

## A Go adapter for portable JSON decisions

The following example isolates the HTTP boundary while leaving policy storage, database uniqueness, and queue insertion to the application. It uses the verified chat-completions route and parses the returned JSON into the same domain type regardless of which compatible service sits behind `baseURL`.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ModerationDecision struct {
	Decision   string   `json:"decision"`
	Confidence float64  `json:"confidence"`
	Reasons    []string `json:"reasons"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func classify(ctx context.Context, client *http.Client, baseURL, token, model, content string) (ModerationDecision, error) {
	payload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Return JSON with decision, confidence, and reasons. Use review when uncertain."},
			{"role": "user", "content": content},
		},
		"response_format": map[string]string{"type": "json_object"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ModerationDecision{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return ModerationDecision{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return ModerationDecision{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ModerationDecision{}, fmt.Errorf("classification transport status %d", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ModerationDecision{}, err
	}

	var envelope chatResponse
	if err := json.Unmarshal(responseBody, &envelope); err != nil || len(envelope.Choices) != 1 {
		return ModerationDecision{}, fmt.Errorf("invalid chat response")
	}
	var decision ModerationDecision
	if err := json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &decision); err != nil {
		return ModerationDecision{}, fmt.Errorf("invalid decision JSON")
	}
	if decision.Decision != "allow" && decision.Decision != "review" && decision.Decision != "block" {
		return ModerationDecision{}, fmt.Errorf("unknown decision")
	}
	if decision.Confidence < 0 || decision.Confidence > 1 || len(decision.Reasons) == 0 {
		return ModerationDecision{}, fmt.Errorf("invalid decision evidence")
	}
	return decision, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = classify(ctx, http.DefaultClient, "https://provider.example", "token", "model", "content")
}
```

The placeholder origin is intentional: deployment configuration chooses the provider, while the domain contract remains local. In production, persist the request metadata and decision under the idempotency key before creating a review task. A retry policy may retry a transport failure, but it should not retry a successfully persisted decision merely because a downstream notification was duplicated.

## Roll out with replay and reconciliation

Start with shadow decisions on a retained, governed sample. Compare provider outputs to human labels, inspect disagreement by language and content category, and record the exact policy and model identifiers. Then enable `review` routing before enabling automatic blocks; this makes queue capacity a visible constraint instead of an emergency discovered after launch.

Measure queue age, duplicate-key attempts, indeterminate results, false-allow reports, false-block appeals, and the count of content revisions without a terminal decision. Reconcile accepted submissions against terminal decisions plus deliberately pending items. A difference of one is an investigation, not harmless eventual consistency.

The changeover test should replay the same revision through both adapters and compare normalized decisions, reasons, and policy IDs. Do not demand identical prose from different models; demand identical domain invariants. The system is ready to switch only when those invariants hold, the review team can absorb the observed queue, and compliance has approved the retention and human-override path.

Keep the boundary explicit.

The conclusion is modest: provider portability is a property of records, validation, and replay—not a promise that every model behaves alike. Keep those boundaries explicit, and a Node.js moderation pipeline can change its inference path without changing the financial audit trail.

## References

- OpenAI Embeddings guide: https://platform.openai.com/docs/guides/embeddings
- LiteLLM, self-hosted LLM gateway: https://github.com/BerriAI/litellm
