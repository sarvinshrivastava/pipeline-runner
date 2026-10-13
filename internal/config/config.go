// Package config loads the service configuration from the environment.
//
// Docker's env_file delivers an unset secret as an EMPTY STRING, not as an
// absent variable, so every reader here treats "" as unset.
package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is the fully validated service configuration.
type Config struct {
	Port        int
	MetricsPort int
	DataDir     string

	APIToken      string
	RunTokenKey   []byte
	EncryptionKey []byte

	SecretManagerURL   string
	SecretManagerToken string

	GitHubToken    string
	GitTokenHosts  []string
	GitAuthorName  string
	GitAuthorEmail string

	NtfyBase      string
	NtfyTopicOK   string
	NtfyTopicFail string
	NtfyToken     string
	DashboardURL  string
	HeartbeatURL  string

	TrustedProxies []netip.Prefix

	MaxHTTPInflight  int
	MaxContainers    int
	PullTimeoutSec   int
	LogMaxBytes      int64
	RunRetentionDays int

	EnableContainerTargets bool
}

// Load reads and validates the configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.Getenv)
}

// LoadFrom reads the configuration through getenv, which tests replace.
func LoadFrom(getenv func(string) string) (Config, error) {
	r := reader{getenv: getenv}
	c := Config{
		Port:        r.int("PORT", 3000),
		MetricsPort: r.int("METRICS_PORT", 9464),
		DataDir:     r.str("DATA_DIR", ""),

		APIToken:      r.str("API_TOKEN", ""),
		RunTokenKey:   r.hexKey("RUN_TOKEN_KEY"),
		EncryptionKey: r.hexKey("ENCRYPTION_KEY"),

		SecretManagerURL:   strings.TrimRight(r.str("SECRET_MANAGER_URL", "http://secret-manager:8000"), "/"),
		SecretManagerToken: r.str("SECRET_MANAGER_TOKEN", ""),

		GitHubToken:    r.str("GITHUB_TOKEN", ""),
		GitTokenHosts:  r.list("GIT_TOKEN_HOSTS", "github.com"),
		GitAuthorName:  r.str("GIT_AUTHOR_NAME", "pipeline-runner"),
		GitAuthorEmail: r.str("GIT_AUTHOR_EMAIL", "pipeline-runner@vps.local"),

		NtfyBase:      normaliseBase(r.str("NTFY_BASE", "https://ntfy.sh")),
		NtfyTopicOK:   r.str("NTFY_TOPIC_OK", ""),
		NtfyTopicFail: r.str("NTFY_TOPIC_FAIL", ""),
		NtfyToken:     r.str("NTFY_TOKEN", ""),
		DashboardURL:  strings.TrimRight(r.str("DASHBOARD_URL", ""), "/"),
		HeartbeatURL:  r.str("HEARTBEAT_URL", ""),

		TrustedProxies: r.prefixes("TRUSTED_PROXIES"),

		MaxHTTPInflight:  r.int("MAX_HTTP_INFLIGHT", 64),
		MaxContainers:    r.int("MAX_CONTAINERS", 4),
		PullTimeoutSec:   r.int("PULL_TIMEOUT_SEC", 300),
		LogMaxBytes:      int64(r.int("LOG_MAX_BYTES", 5*1024*1024)),
		RunRetentionDays: r.int("RUN_RETENTION_DAYS", 60),

		EnableContainerTargets: r.bool("ENABLE_CONTAINER_TARGETS", false),
	}

	errs := r.errs
	if c.DataDir == "" || !filepath.IsAbs(c.DataDir) {
		errs = append(errs, errors.New("DATA_DIR must be an absolute path"))
	}
	if len(c.APIToken) < 32 {
		errs = append(errs, errors.New("API_TOKEN must be at least 32 characters"))
	}
	if c.Port <= 0 || c.MetricsPort <= 0 || c.Port == c.MetricsPort {
		errs = append(errs, errors.New("PORT and METRICS_PORT must be distinct positive ports"))
	}
	if c.MaxHTTPInflight < 1 || c.MaxContainers < 1 {
		errs = append(errs, errors.New("MAX_HTTP_INFLIGHT and MAX_CONTAINERS must be at least 1"))
	}
	if c.RunRetentionDays < 1 {
		errs = append(errs, errors.New("RUN_RETENTION_DAYS must be at least 1"))
	}
	return c, errors.Join(errs...)
}

type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) str(name, fallback string) string {
	if v := strings.TrimSpace(r.getenv(name)); v != "" {
		return v
	}
	return fallback
}

func (r *reader) int(name string, fallback int) int {
	raw := r.str(name, "")
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be an integer", name))
		return fallback
	}
	return n
}

func (r *reader) bool(name string, fallback bool) bool {
	raw := r.str(name, "")
	if raw == "" {
		return fallback
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be true or false", name))
		return fallback
	}
	return b
}

func (r *reader) list(name, fallback string) []string {
	var out []string
	for _, p := range strings.Split(r.str(name, fallback), ",") {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// hexKey reads a required 32-byte key given as 64 hex characters.
func (r *reader) hexKey(name string) []byte {
	raw := r.str(name, "")
	key, err := hex.DecodeString(raw)
	if raw == "" || err != nil || len(key) != 32 {
		r.errs = append(r.errs, fmt.Errorf("%s must be 64 hex characters (32 bytes)", name))
		return nil
	}
	return key
}

// prefixes reads a comma-separated list of CIDRs or bare IPs.
func (r *reader) prefixes(name string) []netip.Prefix {
	var out []netip.Prefix
	for _, p := range r.list(name, "") {
		if !strings.Contains(p, "/") {
			addr, err := netip.ParseAddr(p)
			if err != nil {
				r.errs = append(r.errs, fmt.Errorf("%s: invalid address %q", name, p))
				continue
			}
			out = append(out, netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen()))
			continue
		}
		pf, err := netip.ParsePrefix(p)
		if err != nil {
			r.errs = append(r.errs, fmt.Errorf("%s: invalid CIDR %q", name, p))
			continue
		}
		out = append(out, pf.Masked())
	}
	return out
}

// normaliseBase tolerates a bare host such as "ntfy.example.com".
func normaliseBase(v string) string {
	v = strings.TrimRight(v, "/")
	if v != "" && !strings.Contains(v, "://") {
		v = "https://" + v
	}
	return v
}
