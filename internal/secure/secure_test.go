package secure

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestEncryptRoundTripAndTamper(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	ct, err := Encrypt(key, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(key, ct)
	if err != nil || string(got) != "hello" {
		t.Fatalf("round trip: %q %v", got, err)
	}
	parts := strings.Split(ct, ".")
	parts[3] = base64.RawURLEncoding.EncodeToString([]byte("tampered"))
	if _, err := Decrypt(key, strings.Join(parts, ".")); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("tampered ciphertext err = %v", err)
	}
	if _, err := Decrypt(bytes.Repeat([]byte{2}, 32), ct); !errors.Is(err, ErrDecrypt) {
		t.Fatal("wrong key must fail")
	}
}

func TestRunTokenPerRun(t *testing.T) {
	key := []byte("k")
	a := RunToken(key, "run-a")
	if !VerifyRunToken(key, "run-a", a) || VerifyRunToken(key, "run-b", a) || VerifyRunToken([]byte("other"), "run-a", a) {
		t.Fatal("run token must bind to run and key")
	}
}

func TestAPIToken(t *testing.T) {
	tok, hash, err := NewAPIToken("reel-drop")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, "plr_reel-drop_") || len(tok) != len("plr_reel-drop_")+43 || !bytes.Equal(hash, HashToken(tok)) {
		t.Fatalf("token %q", tok)
	}
	if !EqualDigest("abc", "abc") || EqualDigest("abc", "abcd") {
		t.Fatal("EqualDigest")
	}
}

func TestRedactorEncodingsAndSplits(t *testing.T) {
	secret := "s3cr3t+value/xyz"
	r := NewRedactor([]string{secret, "abc"})
	for _, in := range []string{
		"token=" + secret,
		"b64=" + base64.StdEncoding.EncodeToString([]byte(secret)),
		"url=" + url.QueryEscape(secret),
	} {
		if out := r.String(in); strings.Contains(out, secret) || !strings.Contains(out, Mask) {
			t.Fatalf("not redacted: %q -> %q", in, out)
		}
	}
	if r.String("abc") != "abc" {
		t.Fatal("short values must not be redacted")
	}
	var buf bytes.Buffer
	w := r.Writer(&buf)
	text := "before " + secret + " after"
	for i := 0; i < len(text); i += 3 { // split the secret across writes
		end := min(i+3, len(text))
		if _, err := w.Write([]byte(text[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	if buf.String() != "before "+Mask+" after" {
		t.Fatalf("streamed = %q", buf.String())
	}
}

func TestIPAllowed(t *testing.T) {
	cases := []struct {
		host, ip string
		want     bool
	}{
		{"reel-drop", "127.0.0.1", false},
		{"reel-drop", "169.254.169.254", false},
		{"reel-drop", "::1", false},
		{"reel-drop", "::ffff:127.0.0.1", false},
		{"reel-drop", "::ffff:169.254.169.254", false},
		{"reel-drop", "0.0.0.0", false},
		{"reel-drop", "172.18.0.5", true},
		{"example.com", "10.0.0.5", false},
		{"example.com", "93.184.216.34", true},
		{"api.example.com", "fd00::1", false},
		{"fd00::1", "fd00::1", false},        // private IPv6 literal
		{"10.0.0.5", "10.0.0.5", false},      // private IPv4 literal
		{"", "172.18.0.5", false},            // no requested host: fail closed
		{"example.com", "100.64.1.1", false}, // CGNAT behind a dotted name
		{"reel-drop", "100.64.1.1", true},
		{"example.com", "198.18.0.1", false},
		{"example.com", "255.255.255.255", false},
		{"example.com", "64:ff9b::a00:5", false}, // NAT64 of 10.0.0.5
		{"example.com", "2002:a00:5::1", false},  // 6to4
	}
	for _, c := range cases {
		if got := IPAllowed(c.host, netip.MustParseAddr(c.ip)); got != c.want {
			t.Errorf("IPAllowed(%s, %s) = %v, want %v", c.host, c.ip, got, c.want)
		}
	}
}

func TestHTTPClientRefusesLoopbackAndRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer srv.Close()
	c := NewHTTPClient(2*time.Second, IPAllowed)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	blocked, err := c.Do(req)
	if err == nil {
		_ = blocked.Body.Close()
	}
	if err == nil || !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("loopback must be blocked, err = %v", err)
	}
	open := NewHTTPClient(2*time.Second, func(string, netip.Addr) bool { return true })
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := open.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("redirect must not be followed, status = %d", resp.StatusCode)
	}
}

func TestSign(t *testing.T) {
	got := Sign([]byte("secret"), "1700000000", []byte(`{"a":1}`))
	if !strings.HasPrefix(got, "t=1700000000,v1=") || len(got) != len("t=1700000000,v1=")+64 {
		t.Fatalf("signature %q", got)
	}
}
