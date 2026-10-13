package secure

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
)

// RunToken is the per-run credential a target uses to heartbeat or complete
// an async run. It is valid for that run only.
func RunToken(key []byte, runID string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("plr-run:" + runID))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// VerifyRunToken checks a run token in constant time.
func VerifyRunToken(key []byte, runID, token string) bool {
	return hmac.Equal([]byte(RunToken(key, runID)), []byte(token))
}

// HashToken is how API tokens are stored and looked up.
func HashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// NewAPIToken returns "plr_<name>_<43 base64url chars>" and its hash. The name
// is for humans only; lookup is always by hash.
func NewAPIToken(name string) (token string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	token = "plr_" + name + "_" + base64.RawURLEncoding.EncodeToString(buf)
	return token, HashToken(token), nil
}

// EqualDigest compares two secrets by their SHA-256 digests in constant time,
// so neither content nor length leaks through timing.
func EqualDigest(given, want string) bool {
	a, b := sha256.Sum256([]byte(given)), sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

// Sign returns the X-PLR-Signature value for an HMAC-signed callback.
func Sign(secret []byte, unix string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(unix))
	m.Write([]byte("."))
	m.Write(body)
	return "t=" + unix + ",v1=" + hexEncode(m.Sum(nil))
}

func hexEncode(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2], out[i*2+1] = digits[c>>4], digits[c&0x0f]
	}
	return string(out)
}
