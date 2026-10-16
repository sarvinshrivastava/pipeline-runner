package main

import (
	"bytes"
	"log/slog"
	"net/netip"
	"strings"
	"testing"
)

func TestCheckProxies(t *testing.T) {
	gw := []netip.Addr{netip.MustParseAddr("172.31.250.1")}
	for name, c := range map[string]struct {
		trusted []netip.Prefix
		want    string
	}{
		"match":    {[]netip.Prefix{netip.MustParsePrefix("172.31.250.1/32")}, ""},
		"mismatch": {[]netip.Prefix{netip.MustParsePrefix("10.0.0.1/32")}, "level=ERROR"},
		"none":     {nil, "level=WARN"},
	} {
		var buf bytes.Buffer
		checkProxies(slog.New(slog.NewTextHandler(&buf, nil)), c.trusted, gw)
		out := buf.String()
		if c.want == "" && (strings.Contains(out, "level=ERROR") || strings.Contains(out, "level=WARN")) {
			t.Errorf("%s: unexpected warning: %s", name, out)
		}
		if c.want != "" && !strings.Contains(out, c.want) {
			t.Errorf("%s: want %s in %s", name, c.want, out)
		}
	}
}
