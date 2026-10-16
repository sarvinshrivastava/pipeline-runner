package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

// ErrUnavailable means the vault could not be reached or answered with an error.
var ErrUnavailable = errors.New("secrets manager unavailable")

// ErrMissing means the reference does not exist in the vault.
var ErrMissing = errors.New("secret does not exist")

// ErrNotConfigured means no read token is set.
var ErrNotConfigured = errors.New("SECRET_MANAGER_TOKEN is not configured, so secret references cannot be used")

const cacheTTL = 30 * time.Second

// Client reads the secrets manager at BaseURL with a Bearer read token.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client

	mu       sync.Mutex
	cachedAt time.Time
	keys     map[string]bool
}

// New returns a client with a 10 s request timeout.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: baseURL, Token: token, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) do(ctx context.Context, path string, out any) (int, error) {
	if c.Token == "" {
		return 0, ErrNotConfigured
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnavailable, err) //nolint:errorlint // the transport error is context, not a sentinel
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return resp.StatusCode, nil
	}
	return resp.StatusCode, json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
}

// Get resolves a reference to its value.
func (c *Client) Get(ctx context.Context, ref Ref) (string, error) {
	var body struct {
		Value *string `json:"value"`
	}
	status, err := c.do(ctx, "/api/secrets/"+url.PathEscape(ref.Folder)+"/"+url.PathEscape(ref.Key), &body)
	if err != nil {
		return "", err
	}
	switch {
	case status == http.StatusNotFound:
		return "", fmt.Errorf("%w: %s", ErrMissing, ref)
	case status != http.StatusOK:
		return "", fmt.Errorf("%w: HTTP %d for %s", ErrUnavailable, status, ref)
	case body.Value == nil:
		return "", fmt.Errorf("%w: %s returned no value", ErrUnavailable, ref)
	}
	return *body.Value, nil
}

// List returns every canonical "Folder/KEY" in the vault, sorted. It is
// cached for 30 s; force refetches.
func (c *Client) List(ctx context.Context, force bool) ([]string, error) {
	keys, err := c.keySet(ctx, force)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// Exists reports whether ref exists, refetching once on a miss so a freshly
// added secret is found.
func (c *Client) Exists(ctx context.Context, ref Ref) (bool, error) {
	keys, err := c.keySet(ctx, false)
	if err != nil {
		return false, err
	}
	if keys[ref.String()] {
		return true, nil
	}
	keys, err = c.keySet(ctx, true)
	if err != nil {
		return false, err
	}
	return keys[ref.String()], nil
}

func (c *Client) keySet(ctx context.Context, force bool) (map[string]bool, error) {
	c.mu.Lock()
	if !force && c.keys != nil && time.Since(c.cachedAt) < cacheTTL {
		keys := c.keys
		c.mu.Unlock()
		return keys, nil
	}
	c.mu.Unlock()

	var body struct {
		Keys []struct {
			Key    string `json:"key"`
			Folder string `json:"folder"`
		} `json:"keys"`
	}
	status, err := c.do(ctx, "/api/secrets", &body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%w: HTTP %d listing secrets", ErrUnavailable, status)
	}
	keys := make(map[string]bool, len(body.Keys))
	for _, k := range body.Keys {
		folder := k.Folder
		if folder == "" {
			folder = "Root"
		}
		keys[folder+"/"+k.Key] = true
	}
	c.mu.Lock()
	c.keys, c.cachedAt = keys, time.Now()
	c.mu.Unlock()
	return keys, nil
}

// Ping checks the vault answers a list request.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.keySet(ctx, true)
	return err
}
