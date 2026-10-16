package secure

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSignKnownAnswer pins the X-PLR-Signature format against values computed
// outside this package (openssl dgst -sha256 -hmac secret).
func TestSignKnownAnswer(t *testing.T) {
	cases := []struct {
		secret, unix string
		body         []byte
		want         string
	}{
		{"secret", "1700000000", []byte(`{"a":1}`), "49f24e537407743fa4a0242bb63b94b9a47ee99cbbe071ccd8a22550ae411686"},
		{"secret", "1700000000", nil, "4bc5f74d868b97888288889c5d9d65df02526f94c1592a79fdf4fe8b26e311e5"}, // GET: empty body
	}
	for _, c := range cases {
		if got := Sign([]byte(c.secret), c.unix, c.body); got != "t="+c.unix+",v1="+c.want {
			t.Errorf("Sign(%q, %q, %q) = %s, want v1=%s", c.secret, c.unix, c.body, got, c.want)
		}
	}
	// And against crypto/hmac + encoding/hex for arbitrary bytes.
	secret, body := []byte{0x00, 0xff, 0x10, 'k'}, []byte("\x00\x01binary\xfe")
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("1791882000." + string(body)))
	if got, want := Sign(secret, "1791882000", body), "t=1791882000,v1="+hex.EncodeToString(m.Sum(nil)); got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
}

// recordPolicy remembers every host the dialer asked about.
type recordPolicy struct {
	mu    sync.Mutex
	hosts []string
}

func (p *recordPolicy) allow(host string, _ netip.Addr) bool {
	p.mu.Lock()
	p.hosts = append(p.hosts, host)
	p.mu.Unlock()
	return true
}

func (p *recordPolicy) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.hosts...)
}

func port(t *testing.T, rawURL string) string {
	t.Helper()
	_, p, err := net.SplitHostPort(strings.TrimPrefix(strings.TrimPrefix(rawURL, "https://"), "http://"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDialerPassesRequestedHost checks that the policy sees the name the
// caller asked for (not the resolved IP), and that the request still carries
// that name as Host header and TLS SNI.
func TestDialerPassesRequestedHost(t *testing.T) {
	var gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		w.WriteHeader(204)
	}))
	defer srv.Close()
	pol := &recordPolicy{}
	c := NewHTTPClient(2*time.Second, pol.allow)
	url := "http://localhost:" + port(t, srv.URL) + "/x"
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	hosts := pol.seen()
	if len(hosts) == 0 || hosts[0] != "localhost" {
		t.Fatalf("policy saw %v, want the requested name localhost", hosts)
	}
	if gotHost != "localhost:"+port(t, srv.URL) {
		t.Fatalf("Host header = %q", gotHost)
	}

	// TLS: the SNI is the requested name too.
	var mu sync.Mutex
	var sni string
	tsrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	tsrv.TLS = &tls.Config{GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		mu.Lock()
		sni = hi.ServerName
		mu.Unlock()
		return nil, nil
	}}
	tsrv.StartTLS()
	defer tsrv.Close()
	pol = &recordPolicy{}
	c = NewHTTPClient(2*time.Second, pol.allow)
	// The test certificate does not name localhost; only SNI is under test.
	c.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test server, verifying SNI only
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, "https://localhost:"+port(t, tsrv.URL)+"/", nil)
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if sni != "localhost" {
		t.Fatalf("SNI = %q, want localhost", sni)
	}
	if hosts := pol.seen(); len(hosts) == 0 || hosts[0] != "localhost" {
		t.Fatalf("TLS policy saw %v", hosts)
	}
}

// TestDialerRefusesPerHost: the same IP is allowed for a single-label name and
// refused for a dotted one, so the decision really depends on the passed host.
func TestDialerRefusesPerHost(t *testing.T) {
	d := NewDialer(time.Second, func(host string, _ netip.Addr) bool { return host == "allowed" })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	addr := ln.Addr().String()
	_, p, _ := net.SplitHostPort(addr)
	dial := func(requested string) error {
		ctx := context.WithValue(context.Background(), dialHostKey{}, net.JoinHostPort(requested, p))
		c, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = c.Close()
		}
		return err
	}
	if err := dial("allowed"); err != nil {
		t.Fatalf("allowed host refused: %v", err)
	}
	if err := dial("other.example"); err == nil {
		t.Fatal("refused host dialled")
	}
	// No requested host at all fails closed.
	if c, err := d.DialContext(context.Background(), "tcp", addr); err == nil {
		_ = c.Close()
		t.Fatal("dial without a requested host succeeded")
	}
}
