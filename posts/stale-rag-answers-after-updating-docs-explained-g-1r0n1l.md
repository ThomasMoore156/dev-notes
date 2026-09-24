# Stale RAG Answers After Updating Docs Explained — Go Reindex Debugging

Short answer: keep one authoritative PDF per revision, and make an updated document visible to retrieval only after its new index entries are complete. If an internal wiki assistant still answers from an old PDF, inspect the path from file change to indexed revision to retrieved chunk before changing the prompt. The dominant storage term is usually retained derived data: with N revisions and C chunks per revision, keeping every revision means roughly N × C chunk records, plus their embeddings and any duplicate source files. Retaining only the current revision changes that term to one revision's chunks per document, but sacrifices instant rollback unless the original PDF and index manifest remain available elsewhere.

## What is the bill for a stale answer?

The bill is more than storage. Every retained chunk can enter a candidate set, and every retry that repeats extraction or embedding consumes compute; a missed update also creates an audit problem because the answer may describe a policy that no longer applies. Retrieval-augmented generation grounds an answer in retrieved material, so a correct generation step cannot repair retrieval that returns the old text. The original RAG formulation separates retrieval from generation; that separation gives this investigation its starting point.

Count source bytes, extracted text, chunk records, embeddings, and retained revisions separately. For a deliberately illustrative workload of 100 PDFs with 10 revisions apiece, retaining all revisions produces up to 1,000 revision-level indexing jobs; retaining one current revision per PDF leaves 100 current revision sets. Those figures describe a retention policy, not a measured latency or price. The change that matters is replacing old revision entries atomically at publication time, while preserving a small revision manifest for diagnosis. Re-embedding the same unchanged bytes does not buy freshness.

## Why are RAG answers stale after updating docs?

Trace one known sentence that changed. First compare the file revision recorded by the uploader with the revision seen by the extractor. Then check whether extraction produced the revised sentence, whether chunking included it, whether the indexing job committed, and whether retrieval returned a chunk from that committed revision. A missing job is one possibility. An unchanged filename mistaken for an unchanged file, an extraction failure, a retry that published only half the new chunks, or a stale retrieval cache can look identical from the chat window.

Make the source hash an input to the index job, not a substitute for its completion record. A content digest distinguishes bytes even when a file retains its name; a job keyed by document ID and digest makes duplicate delivery harmless. Keep explicit states such as discovered, extracting, staged, committed, and failed, with timestamps and an error category. A timeout between staging and commit should leave the previous committed revision readable, while the new revision remains unpublished. This resembles a ledger posting: processing and visibility are separate events.

No magic flag fixes it.

That is the limitation of using a content digest as the entire freshness strategy: it tells us whether bytes differ but cannot show whether a job ran, an index committed, or a cache served a previous answer. A scheduled full rebuild avoids reliance on change notifications but can leave updates invisible until the next run, and its repeated extraction is a poor fit when the corpus is large relative to the number of edited documents. Incremental indexing reduces redundant work, at the price of more explicit state transitions and recovery tests. Choose the simpler scheduled job when that visibility delay is acceptable; require revision-aware incremental commits when an updated course PDF must become searchable promptly. Neither approach proves answer correctness by itself.

HTTP conditional requests can reduce transfer when the upstream server supplies validators, but a `304 Not Modified` only speaks for the selected representation under that request; it does not prove that your extractor, queue, or retrieval index is current. Caches also need a defined invalidation point. Record which revision a query used so an operator can distinguish source lag from retrieval ranking and answer generation. Do not print PDF contents into general-purpose logs: a source ID, digest, job ID, committed revision, and chunk IDs usually provide a narrower audit trail.

## How should a reindex commit work?

Treat publication as a revision pointer change. Extract and chunk the candidate PDF, write its derived entries under a new revision ID, verify that the expected entry count was written, and only then switch the document's active revision. Delete obsolete entries after the switch. A worker retry may stage the same revision twice; its write must remain idempotent. The following Go fragment expresses the state transition without assuming a particular queue or search backend:

```go
package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

type Store interface {
	ActiveRevision(context.Context, string) (string, error)
	Stage(context.Context, string, string, []string) error
	Commit(context.Context, string, string, int) error
}

func Publish(ctx context.Context, store Store, documentID string, pdf []byte, chunks []string) error {
	digest := sha256.Sum256(pdf)
	revision := hex.EncodeToString(digest[:])
	active, err := store.ActiveRevision(ctx, documentID)
	if err != nil || active == revision {
		return err
	}
	if err := store.Stage(ctx, documentID, revision, chunks); err != nil {
		return err
	}
	return store.Commit(ctx, documentID, revision, len(chunks))
}
```

The interface contract matters more than the snippet: `Stage` must tolerate repeat calls for the same revision, and `Commit` must verify the count and change the active pointer atomically. In a multi-worker system, commit must also reject an older job if a newer source revision has already been accepted; a digest alone does not order revisions. Maintain a monotonically increasing source sequence or equivalent conditional update. Exactly-once delivery is not required if staging and commit are idempotent, but exactly one revision must be active per document at query time.

## How do you test freshness without sacrificing latency?

Use a tiny evaluation set drawn from actual revision changes: one question whose answer changes, one whose answer stays the same, and one that should receive no supported answer. Record both the retrieved revision IDs and the answer's cited chunk IDs. A passing text match without a matching revision ID is weak evidence; an old passage can happen to produce the new wording. Run the test after publication and again through the normal query path, including its cache. Measure separately the delay from upload to active revision and the query's retrieval latency. Larger chunks, more candidates, and repeated query-time extraction may alter retrieval quality or latency, so compare them against a defined freshness target rather than treating reindex frequency as a universal tuning knob.

On failure, keep the last complete revision serving and expose the pending revision's age to operators. Alert on a growing gap between accepted source sequence and committed sequence, failed extraction, or a commit that never completes. Deployment should preserve the index schema version and replayable job metadata so that a worker upgrade does not silently reinterpret old entries. Access checks still apply to both staged and active data: an index revision is not a substitute for document permissions, and citations are not evidence of regulatory compliance. Retention schedules for source PDFs and diagnostic records require the organization's own legal and policy review.

Freshness has a deadline. Set it explicitly.

The deliberate retention boundary is now clear. Stop keeping obsolete searchable chunks once the replacement is committed; retain the source PDF according to policy and keep a compact manifest mapping document, digest, source sequence, commit time, and job outcome. A failed extraction is cheaper to diagnose with that manifest, but deleting old chunks removes immediate index-level rollback. If rollback is a hard requirement, retain one prior complete revision for a bounded window and account for its extra storage explicitly.

## Further reading

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- HTTP Semantics, conditional requests and validators (RFC 9110): https://www.rfc-editor.org/rfc/rfc9110
- HTTP Caching (RFC 9111): https://www.rfc-editor.org/rfc/rfc9111
