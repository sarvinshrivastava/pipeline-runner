package model

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
)

// SealedEnv holds container env vars without ever exposing values in a
// marshalled form. Its JSON is {"keys":[...],"sealed":"v1.…"}: names in clear,
// values only as ciphertext. Unmarshalling needs no key; only the container
// executor calls Open.
type SealedEnv struct {
	keys   []string
	sealed string
}

type sealedJSON struct {
	Keys   []string `json:"keys"`
	Sealed string   `json:"sealed,omitempty"`
}

// SealEnv encrypts plain under key. An empty map seals to an empty env.
func SealEnv(key []byte, plain map[string]string) (SealedEnv, error) {
	if len(plain) == 0 {
		return SealedEnv{}, nil
	}
	data, err := json.Marshal(plain)
	if err != nil {
		return SealedEnv{}, err
	}
	ct, err := secure.Encrypt(key, data)
	if err != nil {
		return SealedEnv{}, err
	}
	keys := make([]string, 0, len(plain))
	for k := range plain {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return SealedEnv{keys: keys, sealed: ct}, nil
}

// Keys returns the variable names, which API responses may show.
func (e SealedEnv) Keys() []string {
	if e.keys == nil {
		return []string{}
	}
	return append([]string(nil), e.keys...)
}

// Open decrypts the values.
func (e SealedEnv) Open(key []byte) (map[string]string, error) {
	if e.sealed == "" {
		return map[string]string{}, nil
	}
	data, err := secure.Decrypt(key, e.sealed)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, errors.New("sealed env is corrupt")
	}
	return out, nil
}

// MarshalJSON emits names and ciphertext only.
func (e SealedEnv) MarshalJSON() ([]byte, error) {
	return json.Marshal(sealedJSON{Keys: e.Keys(), Sealed: e.sealed})
}

// UnmarshalJSON keeps the ciphertext as is.
func (e *SealedEnv) UnmarshalJSON(b []byte) error {
	var v sealedJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	e.keys, e.sealed = v.Keys, v.Sealed
	return nil
}
