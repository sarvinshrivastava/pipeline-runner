package secure

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"syscall"
	"time"
)

// ErrBlockedAddress is returned when a callback resolves to a forbidden IP.
var ErrBlockedAddress = errors.New("destination address is not allowed")

// IPAllowed applies the callback IP policy to the resolved address.
//   - Loopback, link-local (including cloud metadata), unspecified,
//     multicast and special-use ranges (this-network, reserved, benchmark,
//     NAT64, 6to4) are always refused.
//   - Private, ULA and CGNAT addresses are allowed only for single-label
//     hosts (docker service names such as "reel-drop"). A dotted name or an
//     IP literal that lands there is refused, which also defeats DNS rebinding.
func IPAllowed(host string, ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	for _, p := range alwaysBlocked {
		if p.Contains(ip) {
			return false
		}
	}
	host = strings.TrimSuffix(host, ".")
	_, literalErr := netip.ParseAddr(host)
	singleLabel := host != "" && literalErr != nil && !strings.Contains(host, ".")
	if (ip.IsPrivate() || cgnat.Contains(ip)) && !singleLabel {
		return false
	}
	return true
}

var (
	cgnat         = netip.MustParsePrefix("100.64.0.0/10")
	alwaysBlocked = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("198.18.0.0/15"),
		netip.MustParsePrefix("240.0.0.0/4"), // reserved and broadcast
		netip.MustParsePrefix("64:ff9b::/96"),
		netip.MustParsePrefix("64:ff9b:1::/48"),
		netip.MustParsePrefix("2002::/16"),
	}
)

// IPPolicy decides whether host may connect to ip. Production uses IPAllowed.
type IPPolicy func(host string, ip netip.Addr) bool

// NewDialer returns a dialer that enforces IPAllowed on every connection it
// makes, after DNS resolution, so a hostname cannot be re-pointed between
// validation and dial.
func NewDialer(timeout time.Duration, allow IPPolicy) *net.Dialer {
	return &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		ControlContext: func(ctx context.Context, _, address string, _ syscall.RawConn) error {
			hostPort, _ := ctx.Value(dialHostKey{}).(string)
			host, _, err := net.SplitHostPort(hostPort)
			if err != nil || host == "" {
				// Without the requested name the policy can't tell a docker
				// service from a rebound public name: fail closed.
				return fmt.Errorf("%w: dial without a requested host", ErrBlockedAddress)
			}
			ipStr, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip, err := netip.ParseAddr(ipStr)
			if err != nil {
				return err
			}
			if !allow(host, ip) {
				return fmt.Errorf("%w: %s resolves to %s", ErrBlockedAddress, host, ip)
			}
			return nil
		},
	}
}

type dialHostKey struct{}

// NewHTTPClient returns the client used for callbacks: no proxy, no
// redirects, IP policy enforced at dial time.
func NewHTTPClient(dialTimeout time.Duration, allow IPPolicy) *http.Client {
	d := NewDialer(dialTimeout, allow)
	tr := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return d.DialContext(context.WithValue(ctx, dialHostKey{}, addr), network, addr)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 0, // bounded by the run's timeout context
	}
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
