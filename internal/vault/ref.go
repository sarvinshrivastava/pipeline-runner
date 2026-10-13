// Package vault reads secrets from the VPS secrets manager.
//
// Jobs never store credentials. They store a reference ("Folder/KEY") that is
// resolved at fire time, so rotating a secret in the vault takes effect on the
// next run with no job edit.
package vault

import (
	"fmt"
	"regexp"
	"strings"
)

var keyRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// Ref is a parsed secret reference.
type Ref struct {
	Folder string
	Key    string
}

// String returns the canonical "Folder/KEY" form.
func (r Ref) String() string { return r.Folder + "/" + r.Key }

// ParseRef accepts "Folder/KEY" or a bare "KEY", which defaults to Root.
func ParseRef(raw string) (Ref, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Ref{}, fmt.Errorf("secret reference must not be empty")
	}
	parts := strings.Split(raw, "/")
	if len(parts) > 2 {
		return Ref{}, fmt.Errorf("secret reference %q must be \"KEY\" or \"Folder/KEY\"", raw)
	}
	r := Ref{Folder: "Root", Key: parts[0]}
	if len(parts) == 2 {
		r = Ref{Folder: parts[0], Key: parts[1]}
	}
	if !keyRE.MatchString(r.Folder) || !keyRE.MatchString(r.Key) || r.Folder == "." || r.Folder == ".." || r.Key == "." || r.Key == ".." {
		return Ref{}, fmt.Errorf("invalid secret reference %q", raw)
	}
	return r, nil
}

// ValidPrefix reports whether p is a usable client prefix: a folder ending in
// "/" or a full "Folder/KEY".
func ValidPrefix(p string) bool {
	if strings.HasSuffix(p, "/") {
		return keyRE.MatchString(strings.TrimSuffix(p, "/"))
	}
	r, err := ParseRef(p)
	return err == nil && strings.Contains(p, "/") && r.String() == p
}

// Permitted reports whether ref falls under one of the client's prefixes.
func Permitted(ref Ref, prefixes []string) bool {
	full := ref.String()
	for _, p := range prefixes {
		if strings.HasSuffix(p, "/") && strings.HasPrefix(full, p) {
			return true
		}
		if full == p {
			return true
		}
	}
	return false
}
