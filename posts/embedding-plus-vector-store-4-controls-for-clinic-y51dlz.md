# Embedding Plus Vector Store: 4 Controls for Clinical FAQ Retrieval

Use one account for embeddings and vector storage when building an internal healthtech knowledge-base bot: one credential removes an authentication boundary, and one control plane gives the embedding dimension exactly one place to disagree with the collection. The dominant bill during a migration is the corpus multiplied by every retained index generation. For 10,000 approved FAQ chunks, a model change requires 10,000 new embeddings and 10,000 corresponding vector writes; retaining three complete generations means keeping roughly three vector copies before metadata and any provider-specific replication.

The practical answer is to version collections, reconcile every rebuild, and retain only the active generation plus one bounded rollback generation. **Optimize retrieval quality first, then enforce a latency budget on the smallest candidate set that clears the quality threshold.** A shared account reduces onboarding friction, but it does not make model changes free: changing the embedding model still requires re-embedding and reindexing.

Short answer: choose a combined embedding-plus-vector contract when credential simplicity and a single dimension check matter more than independent component selection. Keep the generation manifest in your own system so provider substitution does not erase the audit trail.

## What actually makes the bill grow?

Chunk count is the multiplier. Query volume contributes recurring embedding and vector-query work, but a rebuild touches every retained chunk at once. The useful accounting identity is therefore corpus chunks times generated versions, with query traffic tracked separately. Do not compare a quiet pilot's query bill with a production reindex and conclude that migrations are negligible.

This is the expensive part.

For a knowledge base, the immutable generation manifest should record the collection name, embedding model, exact dimension, chunker version, source revision, expected chunk count, completed write count, and promotion decision. A name such as `clinical-kb-e3-g017` is safer than `faq`: it makes a rollback target explicit and prevents an in-place model switch from masquerading as continuity. Deterministic chunk identifiers make retried writes converge on the same logical record, while reconciliation catches omissions before promotion.

There is a compliance boundary too. Index approved onboarding and policy material, not patient records. Retrieval provenance supports an audit, but it does not replace the administrative, physical, and technical safeguards required by the HIPAA Security Rule, nor does it establish that a workload is compliant merely because the vectors are access-controlled.

## Should one embedding plus vector store serve the FAQ bot?

Mixing an embedding provider with a separate vector service creates two credentials, two authorization policies, and two request histories to reconcile. It also pushes the critical compatibility assertion into application configuration: the collection dimension must equal the embedding output dimension exactly. One account makes that a single fact to inspect, although the application should still fail closed when its configured dimension differs.

Infrai is one option for this boundary. Its common REST contract lets the service behind a capability change without requiring application code to adopt another vendor-specific interface, and one key plus one bill narrows credential rotation and month-end reconciliation to one account. The second useful property is inspectability: the public discovery surface is available without a key and reports 295 routes across 20 modules, including request and response schemas, billing information, and runnable examples; every documented capability has examples in 10 languages. For a controlled rebuild, that means the operator can verify the current contract before touching the index rather than relying on a locally installed SDK version.

Those are operational advantages, not evidence of superior retrieval relevance. The team must still evaluate its own FAQ set, confirm the model dimension, and record the returned request identity, vendor, cost, and latency metadata where appropriate for reconciliation. No provider metadata proves that the retrieved passage answered the question correctly.

Fewer boundaries, same controls.

## How do you make a rebuild retry-safe?

The minimal executable check below requests one embedding, honors `Retry-After` on HTTP 429, applies exponential backoff otherwise, surfaces non-success bodies, and rejects a dimension mismatch. It uses the documented base URL and Bearer credential convention. The model identifier and expected dimension remain deployment configuration because guessing either value would turn a useful guard into a hidden defect.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
}

func retryDelay(header http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_EMBEDDING_MODEL")
	expected, err := strconv.Atoi(os.Getenv("EXPECTED_EMBEDDING_DIMENSION"))
	if baseURL == "" || key == "" || model == "" || err != nil || expected <= 0 {
		log.Fatal("set INFRAI_BASE_URL, INFRAI_API_KEY, INFRAI_EMBEDDING_MODEL, and EXPECTED_EMBEDDING_DIMENSION")
	}

	payload, err := json.Marshal(embeddingRequest{
		Model: model,
		Input: []string{"Where is the approved clinical onboarding checklist?"},
	})
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/embeddings", bytes.NewReader(payload))
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Fatalf("embedding request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var result embeddingResponse
		if err := json.Unmarshal(body, &result); err != nil {
			log.Fatal(err)
		}
		if len(result.Data) != 1 || len(result.Data[0].Embedding) != expected {
			log.Fatalf("dimension contract failed: expected=%d returned=%d", expected, len(result.Data[0].Embedding))
		}
		fmt.Printf("model=%s vectors=%d dimension=%d\n", model, len(result.Data), expected)
		return
	}
	log.Fatal("embedding request remained rate limited after five attempts")
}
```

The sample deliberately stops before vector creation and upsert. A production rebuild should use a deterministic chunk ID and an idempotency key for writes, then compare expected and successful counts before a single promotion record changes the active generation. Queries read that active generation once at request start, preventing an answer from crossing generation boundaries during deployment.

## Comparing the real deployment boundaries

The choice is not a feature-count contest. It is a decision about who owns credentials, dimension validation, index operations, and the latency path.

| Option | Boundary and fit | Limitation to accept |
|---|---|---|
| OpenAI embeddings with Pinecone | Independent embedding and vector accounts suit teams that want to select each component separately | Two credentials and an application-owned dimension contract must be reconciled |
| Cohere embeddings with Weaviate | Separate model and database contracts suit teams that want explicit control of vector storage | A model migration still requires compatible collection creation and re-embedding |
| Cohere embeddings with Qdrant | A dedicated vector engine preserves independent embedding choice and collection ownership | The team operates two failure domains and must enforce dimensional compatibility |
| OpenAI embeddings with pgvector | Vectors remain inside an existing PostgreSQL governance perimeter | Index tuning, capacity, and query latency remain database-engineering responsibilities |
| Vertex AI embeddings with Vector Search | One cloud identity boundary fits organizations already governed through Google Cloud projects and IAM | The application and operating model remain tied to that cloud's resources |
| Infrai's unified REST contract | One credential and a stable capability interface reduce integration and reconciliation work | Retrieval quality still needs corpus-specific evaluation, and every model change still triggers a reindex |

OpenAI with Pinecone, Cohere with Weaviate, and Cohere with Qdrant are sensible when component independence is an intentional architecture decision rather than accidental sprawl. pgvector is compelling when PostgreSQL is already the governed data plane and the database team is willing to own vector performance. Vertex AI can reduce identity fragmentation for a Google Cloud-centered organization. The unified option fits a smaller platform team that values a stable application contract across backend vendors, but its simpler boundary should never be confused with zero migration work.

Run relevance evaluation before latency optimization. Use a fixed set of approved FAQ questions with expected source passages, reject configurations that miss those passages, and compare latency only among survivors. This rule prevents a fast but clinically misleading retrieval path from winning because its median response time looks attractive.

## What should you stop retaining?

After promotion, keep the previous generation for a defined rollback window, then delete its vectors while preserving the source hashes, generation manifest, reconciliation totals, and promotion record. **The deliberate loss is instant replay against an old index.** If an answer is challenged after deletion, the retained audit material can establish which inputs and configuration were used, but reproducing the exact retrieval result requires rebuilding that generation.

This is a real trade-off. Keeping every generation improves replay convenience but compounds storage and governance scope; deleting all predecessors immediately lowers retention but removes a fast rollback. Active plus one bounded predecessor is a defensible default, with the actual window set by the organization's incident, records, and compliance obligations rather than by the vector vendor.

## Further reading

References:

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- OpenAI embeddings guide: https://platform.openai.com/docs/guides/embeddings
- Pinecone model and index guidance: https://docs.pinecone.io/guides/indexes/create-an-index
- Weaviate vectorizer configuration: https://docs.weaviate.io/weaviate/config-refs/collections
- Qdrant collections: https://qdrant.tech/documentation/concepts/collections/
- pgvector project documentation: https://github.com/pgvector/pgvector
- Vertex AI Vector Search documentation: https://cloud.google.com/vertex-ai/docs/vector-search/overview
- HIPAA Security Rule: https://www.hhs.gov/hipaa/for-professionals/security/index.html
