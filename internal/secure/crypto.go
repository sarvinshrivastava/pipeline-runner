// Package secure holds the service's cryptographic and network-safety
// primitives: at-rest encryption, run tokens, API tokens, secret redaction and
// the SSRF-safe dialer.
package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
)

// Ciphertext format, ported from the TS service: "v1.<iv>.<tag>.<ct>", each
// part base64url. The version prefix leaves room for a key id later.
const cipherVersion = "v1"

var b64 = base64.RawURLEncoding

// ErrDecrypt is returned for any malformed or tampered ciphertext.
var ErrDecrypt = errors.New("decrypt failed")

// Encrypt seals plaintext with AES-256-GCM under a 32-byte key.
func Encrypt(key, plaintext []byte) (string, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nil, iv, plaintext, nil)
	ct, tag := sealed[:len(sealed)-gcm.Overhead()], sealed[len(sealed)-gcm.Overhead():]
	return strings.Join([]string{cipherVersion, b64.EncodeToString(iv), b64.EncodeToString(tag), b64.EncodeToString(ct)}, "."), nil
}

// Decrypt opens a value produced by Encrypt.
func Decrypt(key []byte, value string) ([]byte, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 4 || parts[0] != cipherVersion {
		return nil, ErrDecrypt
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	iv, err1 := b64.DecodeString(parts[1])
	tag, err2 := b64.DecodeString(parts[2])
	ct, err3 := b64.DecodeString(parts[3])
	if err1 != nil || err2 != nil || err3 != nil || len(iv) != gcm.NonceSize() || len(tag) != gcm.Overhead() {
		return nil, ErrDecrypt
	}
	plain, err := gcm.Open(nil, iv, append(ct, tag...), nil)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("encryption key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
