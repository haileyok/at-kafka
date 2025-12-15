package atkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/pkg/robusthttp"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

type PlcClient struct {
	client     *http.Client
	dir        *identity.BaseDirectory
	plcHost    string
	docCache   *lru.LRU[string, *identity.DIDDocument]
	auditCache *lru.LRU[string, *DidAuditEntry]
}

type PlcClientArgs struct {
	PlcHost string
}

func NewPlcClient(args *PlcClientArgs) *PlcClient {
	client := robusthttp.NewClient(robusthttp.WithMaxRetries(2))
	client.Timeout = 3 * time.Second

	baseDir := identity.BaseDirectory{
		PLCURL:     args.PlcHost,
		PLCLimiter: rate.NewLimiter(rate.Limit(200), 100),
		HTTPClient: *client,
		Resolver: net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				dialer := net.Dialer{Timeout: time.Second * 5}
				nameserver := address
				return dialer.DialContext(ctx, network, nameserver)
			},
		},
		TryAuthoritativeDNS: true,
		// primary Bluesky PDS instance only supports HTTP resolution method
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}

	docCache := lru.NewLRU(100_000, func(_ string, _ *identity.DIDDocument) {
		cacheSize.WithLabelValues("did_doc").Dec()
	}, 5*time.Minute)

	auditCache := lru.NewLRU(100_000, func(_ string, _ *DidAuditEntry) {
		cacheSize.WithLabelValues("audit_log").Dec()
	}, 1*time.Hour)

	return &PlcClient{
		client:     client,
		dir:        &baseDir,
		plcHost:    args.PlcHost,
		docCache:   docCache,
		auditCache: auditCache,
	}
}

type OperationService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

type DidLogEntry struct {
	Sig                 string                      `json:"sig"`
	Prev                *string                     `json:"prev"`
	Type                string                      `json:"type"`
	Services            map[string]OperationService `json:"services"`
	AlsoKnownAs         []string                    `json:"alsoKnownAs"`
	RotationKeys        []string                    `json:"rotationKeys"`
	VerificationMethods map[string]string           `json:"verificationMethods"`
}

type DidAuditEntry struct {
	Did       string      `json:"did"`
	Operation DidLogEntry `json:"operation"`
	Cid       string      `json:"cid"`
	Nullified bool        `json:"nullified"`
	CreatedAt string      `json:"createdAt"`
}

type DidAuditLog []DidAuditEntry

func (c *PlcClient) GetDIDDoc(ctx context.Context, did string) (*identity.DIDDocument, error) {
	status := "error"
	cached := false

	defer func() {
		plcRequests.WithLabelValues("did_doc", status, fmt.Sprintf("%t", cached)).Inc()
	}()

	if val, ok := c.docCache.Get(did); ok {
		status = "ok"
		cached = true
		return val, nil
	}

	didDoc, err := c.dir.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return nil, fmt.Errorf("failed to lookup DID: %w", err)
	}

	if didDoc == nil {
		return nil, fmt.Errorf("DID Document not found")
	}

	if c.docCache != nil {
		c.docCache.Add(did, didDoc)
	}

	cacheSize.WithLabelValues("did_doc").Inc()
	status = "ok"

	return didDoc, nil
}

var ErrAuditLogNotFound = errors.New("audit log not found for DID")

func (c *PlcClient) GetDIDAuditLog(ctx context.Context, did string) (*DidAuditEntry, error) {
	status := "error"
	cached := false

	defer func() {
		plcRequests.WithLabelValues("audit_log", status, fmt.Sprintf("%t", cached)).Inc()
	}()

	if val, ok := c.auditCache.Get(did); ok {
		status = "ok"
		cached = true
		return val, nil
	}

	ustr := fmt.Sprintf("%s/%s/log/audit", c.plcHost, did)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ustr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request for DID audit log: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DID audit log: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrAuditLogNotFound
		}
		return nil, fmt.Errorf("DID audit log fetch returned unexpected status: %s", resp.Status)
	}

	var entries []DidAuditEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to read DID audit log response bytes into DidAuditLog: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("empty did audit log for did %s", did)
	}

	entry := entries[0]

	if c.auditCache != nil {
		c.auditCache.Add(did, &entry)
	}

	cacheSize.WithLabelValues("audit_log").Inc()
	status = "ok"

	return &entry, nil
}
