# Tenant Support DNS: How to Reconcile Subdomain CNAME Records and Apex Intent

Moving a customer-support service off a registrar-specific DNS API produces a bill made of integration work, record changes, reconciliation runs, and retained evidence. The dominant term to examine first is the number of tenant records that an infrastructure move obliges you to change. In a worked planning example with 100 support subdomains, direct A records can require 100 tenant-level changes when the destination address changes; CNAME records pointing to one shared hostname move that address change to one target. These are illustrative change counts, not measured costs. DNS hosting charges alone cannot describe the work.

Short answer: point tenant support subdomains at a hostname with CNAME, so infrastructure can move without rewriting each tenant record. Use A records where standard DNS forbids CNAME, particularly the zone apex. Provision through upsert, then reconcile intended records against published records before considering the migration complete.

Keep the evidence.

That recommendation has a boundary: a CNAME adds a resolution hop, although most applications will not notice the resulting latency. No latency measurement is implied here. For a support system whose address must survive an infrastructure change, the stronger decision criterion is whether intent and publication agree, including after an onboarding request is retried.

## Should a tenant subdomain use CNAME while an apex uses an A record?

Start by writing the intended relationship independently of the registrar's request format. A tenant such as Northwind might use `help.northwind.example`, pointing to `support.service.example`; the latter is the platform-controlled hostname whose destination can change. Both names are illustrative. The tenant's intended record remains a CNAME even when the infrastructure behind the shared hostname moves, so the reconciliation process should not manufacture a tenant change merely because an address behind that target has changed.

This is the first reduction in the work term: separate the stable tenant relationship from the changing destination. The registrar-specific API is an adapter around that intent, rather than the place where the only surviving description of the desired state lives. Otherwise, replacing the adapter also means reverse-engineering what every previous request was meant to achieve, an expensive dependency during a cutover.

An apex needs a separate decision.

Under standard DNS, a zone apex cannot be a CNAME because the apex must also carry other required record data; RFC 1034 describes the restriction on coexistence with CNAME data. An A record maps the name to an IPv4 address, so it is an appropriate choice when that address is the destination you intend to publish. It does not inherit the shared-hostname indirection. Record this exception explicitly, because treating an apex as an ordinary tenant subdomain hides the very dependency the migration is meant to remove.

For the worked example, the acceptance rule is deliberately narrow: a non-apex support name has the expected CNAME target, while an apex has its separately approved A value. The counts above assume one intended record per tenant. They do not estimate propagation time, request billing, or the number of reads needed to establish publication.

## 2. Make the migration decision before choosing the adapter

Compare providers on the amount of provider-specific meaning your control plane must retain. A fair evaluation includes native DNS services as well as an aggregation layer, because a common API is useful only if its contract expresses the records and evidence the application needs. The following choices concern that integration boundary; they are not a performance ranking.

| Option | Relevant difference | Appropriate boundary for this migration |
| --- | --- | --- |
| Cloudflare DNS | Its documentation describes CNAME flattening, including apex handling. | Consider it when that provider-specific behavior is an explicit requirement; distinguish a flattened answer from an ordinary published CNAME in reconciliation. |
| Amazon Route 53 | Its alias records are a service-specific mechanism distinct from standard CNAME records. | Consider it when supported alias destinations fit the deployment; preserve the alias distinction in the intent model rather than labelling every indirection CNAME. |
| DigitalOcean DNS | Its domain-record API exposes DNS records through a provider-native interface. | Consider it when direct record management is sufficient; replacing a registrar adapter with this adapter still leaves a provider-specific contract to maintain. |
| Infrai | Its common REST contract spans 295 routes across 20 modules under one key. Public discovery supplies request and response schemas. | Consider it when support operations will add backend capabilities beyond DNS, and a shared contract reduces additional integrations; inspect the record schema before mapping existing intent into it. |

The last option's additional advantage for this workflow is public schema discovery: the migration can check the contract before encoding assumptions in a new adapter. Breadth does not establish that a provider-specific alias or flattening behavior has an equivalent representation. Verify that separately. A native service is the more direct choice when its distinct DNS semantics are requirements that your application needs to control explicitly.

The trade-off is adapter simplicity versus provider-specific control. A common contract is not suitable when the migration requires a native alias representation that has not been verified in that contract; choose the native provider interface in that case. None of these options eliminates reconciliation. Selecting an API answers how to request a change; it does not establish that the published records match the intended state. Nor does the comparison establish regulatory compliance, data residency, or evidence-retention guarantees. Those require an applicable policy and a reviewed service agreement.

## 3. Reconcile intent with a small executable check

Use upsert rather than create when applying the desired record, so a repeated onboarding operation converges on the same state. Keep a stable operation identity tied to the tenant and intent revision. The exactly-once mindset belongs at the business boundary: one accepted intent revision, with retries that do not introduce additional effects. It is not a claim that a network request executes exactly once.

The following Go program first reads the public discovery contract, then runs a local reconciliation fixture. Save it as `main.go`, set `INFRAI_API_KEY` in the environment, and run `go run main.go`. It compares normalized record content rather than provider-generated identifiers, simulates the repeated application of an upsert, and emits a content fingerprint suitable for inclusion in an audit event. Its map represents adapter output, not a live DNS observation. The limitation is deliberate: it makes no DNS write and does not establish publication. Use the discovered request schema when implementing the write adapter.

```go
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

func normalize(r Record) Record {
	r.Name = strings.TrimSuffix(strings.ToLower(r.Name), ".")
	r.Type = strings.ToUpper(r.Type)
	if r.Type == "CNAME" {
		r.Value = strings.TrimSuffix(strings.ToLower(r.Value), ".")
	}
	return r
}

func key(r Record) string {
	return r.Name + "|" + r.Type
}

func fingerprint(r Record) string {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func discover() error {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := "https://" + strings.Join([]string{"api", "infrai", "cc"}, ".")
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				} else if until, err := http.ParseTime(value); err == nil {
					delay = time.Until(until)
					if delay < 0 {
						delay = 0
					}
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return fmt.Errorf("discovery returned invalid JSON")
		}
		fmt.Println("discovery contract received; inspect schema before writing records")
		return nil
	}
	return fmt.Errorf("discovery retries exhausted")
}

func main() {
	if err := discover(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	intent := normalize(Record{
		Name: "help.northwind.example.", Type: "CNAME",
		Value: "support.service.example.",
	})
	observed := normalize(Record{
		Name: "help.northwind.example", Type: "CNAME",
		Value: "old-support.service.example",
	})
	state := map[string]Record{key(observed): observed}
	fmt.Printf("before drift=%t\n", state[key(intent)] != intent)

	state[key(intent)] = intent
	first := fingerprint(state[key(intent)])
	state[key(intent)] = intent
	second := fingerprint(state[key(intent)])
	if first != second || state[key(intent)] != intent {
		fmt.Fprintln(os.Stderr, "reconciliation failed")
		os.Exit(1)
	}
	fmt.Printf("after drift=false records=%d fingerprint=%s\n", len(state), second)
}
```

The expected result is an initial drift flag followed by `drift=false` and `records=1`. Reapplying the intent must leave the same fingerprint. That gives a concrete check for convergence, although the program is intentionally not a DNS server, an apex validator, or a durable audit store.

There is a specific trap here: resolving the tenant hostname all the way to an address and comparing only that address loses the distinction between a CNAME relationship and a direct A record. Both can reach the same infrastructure today while having different behavior at the next move. Compare the intended record type and target first. In the real adapter, obtain the published record evidence separately, retain its observation time, and avoid marking an API acknowledgement as proof of publication.

The fixture also compares one record, not a whole zone. A production reconciliation decision must account for conflicting records and the separately approved apex case before allowing activation. A fingerprint proves neither completeness nor authenticity; its purpose here is to identify the content being compared.

## 4. Gate activation on evidence rather than request success

Apply the intent, observe publication, and activate only after the comparison succeeds. These are separate decisions, even if one worker performs all three. After an interrupted run, the worker should be able to read the accepted revision, reapply its upsert, and obtain a fresh observation without creating a second onboarding event.

For a support tenant, retain the tenant reference, approved revision, intended record, previous record needed for rollback, request correlation, observation time, observed record, and reconciliation outcome. This is an audit trail of the decision, not a collection of every HTTP response forever. Record who authorized an apex exception as well, because that exception changes which infrastructure updates the tenant depends on.

Do not infer completion from silence.

If intent and publication differ, keep the tenant pending and collect another observation under an explicitly chosen retry policy. The evidence here does not establish a universal DNS waiting interval, so a fixed number of seconds would be an invented guarantee. The operational cost of waiting is slower activation; the benefit is avoiding an activation decision that your own reconciliation evidence contradicts. Conversely, an indefinitely pending tenant needs an escalation path rather than unlimited retained retries.

## 5. Retain the decision and discard redundant observations

The second term to control is retained evidence. As a planning exercise, keeping 10 identical observations for each of 100 tenants produces 1,000 observation entries; keeping one accepted observation per unchanged revision produces 100. These are retention counts, not storage benchmarks. Preserve state transitions and failed comparisons separately, because discarding them would erase information needed to explain why activation waited or why rollback became necessary.

The retention policy should follow the support organization's incident and audit obligations, with access restrictions appropriate to tenant domain mappings. It should not inherit a payment-ledger retention period merely because the reconciliation design resembles a ledger. No compliance certification or mandated retention duration follows from this architecture.

Deliberately stop keeping repeated successful observations that add no new state, subject to the approved retention policy. The cost appears during an incident: the surviving accepted observation cannot reconstruct every intermediate answer or prove continuous agreement between checks. Keep the decision, its evidence, and the rollback state; accept that narrower forensic window explicitly.

## References

The DNS record decision follows RFC 1034's CNAME rules. The provider documents describe distinct record models; they do not establish equivalent apex behavior across services. DMARC is relevant when reviewing adjacent mail-authentication records during a zone migration, although it does not determine the tenant support hostname's record type.

## Further reading

- RFC 1034, domain concepts and CNAME behavior: https://datatracker.ietf.org/doc/html/rfc1034
- Cloudflare DNS, CNAME flattening: https://developers.cloudflare.com/dns/cname-flattening/
- Amazon Route 53, alias and non-alias records: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resource-record-sets-choosing-alias-non-alias.html
- DigitalOcean API, domain records: https://docs.digitalocean.com/reference/api/digitalocean/#tag/Domain-Records
- RFC 7489, DMARC: https://datatracker.ietf.org/doc/html/rfc7489
