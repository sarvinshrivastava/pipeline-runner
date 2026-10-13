package config

import (
	"strings"
	"testing"
)

func validEnv() map[string]string {
	return map[string]string{
		"DATA_DIR":        "/data",
		"API_TOKEN":       strings.Repeat("a", 32),
		"RUN_TOKEN_KEY":   strings.Repeat("ab", 32),
		"ENCRYPTION_KEY":  strings.Repeat("cd", 32),
		"TRUSTED_PROXIES": "none",
	}
}

func load(env map[string]string) (Config, error) {
	return LoadFrom(func(k string) string { return env[k] })
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(validEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 3000 || c.MetricsPort != 9464 || c.MaxHTTPInflight != 64 || c.MaxContainers != 4 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.EnableContainerTargets {
		t.Fatal("container targets must be off by default")
	}
	if len(c.GitTokenHosts) != 1 || c.GitTokenHosts[0] != "github.com" {
		t.Fatalf("GitTokenHosts = %v", c.GitTokenHosts)
	}
}

func TestEmptyStringIsUnset(t *testing.T) {
	env := validEnv()
	env["PORT"] = ""
	env["NTFY_BASE"] = ""
	c, err := load(env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 3000 || c.NtfyBase != "https://ntfy.sh" {
		t.Fatalf("empty env not treated as unset: %+v", c)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]func(map[string]string){
		"missing data dir":   func(e map[string]string) { delete(e, "DATA_DIR") },
		"relative data dir":  func(e map[string]string) { e["DATA_DIR"] = "data" },
		"short api token":    func(e map[string]string) { e["API_TOKEN"] = "short" },
		"missing run key":    func(e map[string]string) { delete(e, "RUN_TOKEN_KEY") },
		"short enc key":      func(e map[string]string) { e["ENCRYPTION_KEY"] = "abcd" },
		"non-hex key":        func(e map[string]string) { e["RUN_TOKEN_KEY"] = strings.Repeat("zz", 32) },
		"bad int":            func(e map[string]string) { e["PORT"] = "abc" },
		"same ports":         func(e map[string]string) { e["METRICS_PORT"] = "3000" },
		"bad proxy":          func(e map[string]string) { e["TRUSTED_PROXIES"] = "nope" },
		"no proxy setting":   func(e map[string]string) { delete(e, "TRUSTED_PROXIES") },
		"port too high":      func(e map[string]string) { e["PORT"] = "70000" },
		"bad bool":           func(e map[string]string) { e["ENABLE_CONTAINER_TARGETS"] = "maybe" },
		"zero pool":          func(e map[string]string) { e["MAX_HTTP_INFLIGHT"] = "0" },
		"zero retention day": func(e map[string]string) { e["RUN_RETENTION_DAYS"] = "0" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			env := validEnv()
			mutate(env)
			if _, err := load(env); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestTrustedProxies(t *testing.T) {
	env := validEnv()
	env["TRUSTED_PROXIES"] = "172.31.250.1, 10.0.0.0/8"
	c, err := load(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.TrustedProxies) != 2 || c.TrustedProxies[0].Bits() != 32 {
		t.Fatalf("TrustedProxies = %v", c.TrustedProxies)
	}
}

func TestNtfyBareHost(t *testing.T) {
	env := validEnv()
	env["NTFY_BASE"] = "ntfy.example.com/"
	c, err := load(env)
	if err != nil {
		t.Fatal(err)
	}
	if c.NtfyBase != "https://ntfy.example.com" {
		t.Fatalf("NtfyBase = %q", c.NtfyBase)
	}
}
