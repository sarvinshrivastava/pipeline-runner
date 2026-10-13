package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"net/netip"
	"os"
	"strings"
)

// readGateways lists the IPv4 default-route gateways from /proc/net/route.
// Outside Linux it returns nothing and the proxy check is skipped.
func readGateways() []netip.Addr {
	f, err := os.Open("/proc/net/route")
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	var out []netip.Addr
	sc := bufio.NewScanner(f)
	sc.Scan() // header
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 || fields[2] == "00000000" {
			continue
		}
		b, err := hex.DecodeString(fields[2])
		if err != nil || len(b) != 4 {
			continue
		}
		var ip [4]byte
		binary.BigEndian.PutUint32(ip[:], binary.LittleEndian.Uint32(b))
		out = append(out, netip.AddrFrom4(ip))
	}
	return out
}

// checkProxies logs the gateways and errors when TRUSTED_PROXIES matches none
// of them: published-port traffic then shares one lockout bucket.
func checkProxies(log *slog.Logger, trusted []netip.Prefix, gateways []netip.Addr) {
	if len(gateways) == 0 {
		return
	}
	log.Info("container gateways", "gateways", gateways)
	if len(trusted) == 0 {
		log.Warn("TRUSTED_PROXIES is none: X-Forwarded-For is ignored and proxied callers share one lockout bucket")
		return
	}
	for _, p := range trusted {
		for _, g := range gateways {
			if p.Contains(g) {
				return
			}
		}
	}
	log.Error("no TRUSTED_PROXIES entry matches a container gateway; proxied callers will share one lockout bucket",
		"trusted", trusted, "gateways", gateways)
}
