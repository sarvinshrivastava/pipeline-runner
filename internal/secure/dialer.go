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
//   - Loopback, link-local (including cloud metadata), unspecified and
//     multicast addresses are always refused.
//   - Private and ULA addresses are allowed only for single-label hosts
//     (docker service names such as "reel-drop"). A dotted public name that
//     resolves privately is treated as DNS rebinding and refused.
func IPAllowed(host string, ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	if ip.IsPrivate() && strings.Contains(strings.TrimSuffix(host, "."), ".") {
		return false
	}
	return true
}

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
			host, _, _ := net.SplitHostPort(hostPort)
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
