// Package api is the HTTP surface: /v1 (OpenAPI-described), /healthz, the run
// callbacks and the static dashboard.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/netip"
	"runtime/debug"
	"strings"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

// maxBodyBytes caps every request body (payloads are ≤ 256 KiB).
const maxBodyBytes = 512 << 10

// SecretLister lists vault keys.
type SecretLister interface {
	List(ctx context.Context, force bool) ([]string, error)
	Ping(ctx context.Context) error
}

// Deps wires the API.
type Deps struct {
	Engine         *engine.Engine
	Store          *store.Store
	Secrets        SecretLister
	Log            *slog.Logger
	APIToken       string
	RunTokenKey    []byte
	TrustedProxies []netip.Prefix
	Version        string
	Static         http.Handler // dashboard; nil disables it
	DockerStatus   func(ctx context.Context) string
	Now            func() time.Time
}

// Server is the API handler.
type Server struct {
	d       Deps
	mux     *http.ServeMux
	limiter *limiter
	routes  []route
}

// New builds the handler.
func New(d Deps) *Server {
	if d.Now == nil {
		d.Now = time.Now
	}
	s := &Server{d: d, mux: http.NewServeMux(), limiter: newLimiter(10, 5*time.Minute, 10000, d.Now)}
	s.routes = s.routeTable()
	for _, rt := range s.routes {
		if rt.static && d.Static == nil {
			continue
		}
		s.mux.Handle(rt.method+" "+rt.pattern, s.wrap(rt))
	}
	return s
}

// ServeHTTP applies the outer middleware and turns unmatched requests into
// JSON 404/405 instead of the mux's plain-text replies.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if v := recover(); v != nil {
			s.d.Log.Error("panic in handler", "panic", v, "stack", string(debug.Stack()))
			writeError(w, http.StatusInternalServerError, "internal", "internal error")
		}
	}()
	securityHeaders(w, r)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if _, pattern := s.mux.Handler(r); pattern == "" {
		if s.methodExists(r) {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "no such route")
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) methodExists(r *http.Request) bool {
	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete, http.MethodPut} {
		probe := r.Clone(r.Context())
		probe.Method = m
		if _, p := s.mux.Handler(probe); p != "" {
			return true
		}
	}
	return false
}

func securityHeaders(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
	if strings.HasPrefix(r.URL.Path, "/v1/") {
		h.Set("Cache-Control", "no-store")
	}
}

type ctxKey int

const principalKey ctxKey = iota

func principalFrom(r *http.Request) model.Principal {
	p, _ := r.Context().Value(principalKey).(model.Principal)
	return p
}

// isBootstrap reports whether p is the bootstrap admin token, which belongs
// to no client.
func isBootstrap(p model.Principal) bool { return p.Admin && p.ClientID == "" }

// wrap applies per-route auth, limiting and scope checks.
func (s *Server) wrap(rt route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r, s.d.TrustedProxies)
		switch rt.auth {
		case authNone:
		case authRunToken:
			if s.limiter.blocked(ip) {
				writeError(w, http.StatusTooManyRequests, "rate_limited", "too many failed attempts")
				return
			}
			tok := r.Header.Get("X-PLR-Run-Token")
			if tok == "" || !secure.VerifyRunToken(s.d.RunTokenKey, r.PathValue("id"), tok) {
				s.limiter.fail(ip)
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid run token")
				return
			}
		case authBearer:
			if s.limiter.blocked(ip) {
				writeError(w, http.StatusTooManyRequests, "rate_limited", "too many failed attempts")
				return
			}
			p, ok := s.authenticate(r)
			if !ok {
				s.limiter.fail(ip)
				w.Header().Set("WWW-Authenticate", `Bearer realm="pipeline-runner"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid token")
				return
			}
			if rt.scope != "" && !p.HasScope(rt.scope) {
				writeError(w, http.StatusForbidden, "forbidden", "token lacks scope "+rt.scope)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), principalKey, p))
		}
		rt.handler(w, r)
	})
}

func (s *Server) authenticate(r *http.Request) (model.Principal, bool) {
	h := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		return model.Principal{}, false
	}
	if secure.EqualDigest(tok, s.d.APIToken) {
		return model.Principal{Name: "admin", Admin: true, Scopes: model.AllScopes}, true
	}
	c, err := store.ClientByTokenHash(r.Context(), s.d.Store.R, secure.HashToken(tok), s.d.Now())
	if err != nil {
		return model.Principal{}, false
	}
	return model.PrincipalOf(c), true
}

// clientIP is the right-most X-Forwarded-For hop that is not a trusted
// proxy, honoured only when the direct peer is itself trusted.
func clientIP(r *http.Request, trusted []netip.Prefix) string {
	peer, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	addr := peer.Addr().Unmap()
	if !isTrusted(addr, trusted) {
		return addr.String()
	}
	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break
		}
		hop = hop.Unmap()
		if !isTrusted(hop, trusted) {
			return hop.String()
		}
		addr = hop
	}
	return addr.String()
}

func isTrusted(a netip.Addr, trusted []netip.Prefix) bool {
	for _, p := range trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
