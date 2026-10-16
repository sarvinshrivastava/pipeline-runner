package vault

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func server(t *testing.T) (*httptest.Server, *int) {
	lists := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/secrets":
			lists++
			_, _ = w.Write([]byte(`{"keys":[{"folder":"ReelDrop","key":"PLR_TOKEN"},{"folder":"","key":"ROOTKEY"}]}`))
		case "/api/secrets/ReelDrop/PLR_TOKEN":
			_, _ = w.Write([]byte(`{"value":"v-123"}`))
		case "/api/secrets/ReelDrop/NOVALUE":
			_, _ = w.Write([]byte(`{}`))
		case "/api/secrets/ReelDrop/BROKEN":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &lists
}

func TestParseRef(t *testing.T) {
	r, err := ParseRef("KEY")
	if err != nil || r.String() != "Root/KEY" {
		t.Fatalf("bare key: %v %v", r, err)
	}
	for _, bad := range []string{"", "a/b/c", "a b/c", "../x", "x/.."} {
		if _, err := ParseRef(bad); err == nil {
			t.Errorf("ParseRef(%q) accepted", bad)
		}
	}
	ref, _ := ParseRef("ReelDrop/KEY")
	if !Permitted(ref, []string{"ReelDrop/"}) || Permitted(ref, []string{"Reel"}) || !Permitted(ref, []string{"ReelDrop/KEY"}) {
		t.Fatal("Permitted")
	}
	if !ValidPrefix("ReelDrop/") || !ValidPrefix("ReelDrop/KEY") || ValidPrefix("ReelDrop") {
		t.Fatal("ValidPrefix")
	}
}

func TestGetAndErrors(t *testing.T) {
	srv, _ := server(t)
	c := New(srv.URL, "tok")
	ctx := context.Background()
	if v, err := c.Get(ctx, Ref{"ReelDrop", "PLR_TOKEN"}); err != nil || v != "v-123" {
		t.Fatalf("get: %q %v", v, err)
	}
	if _, err := c.Get(ctx, Ref{"ReelDrop", "GONE"}); !errors.Is(err, ErrMissing) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := c.Get(ctx, Ref{"ReelDrop", "BROKEN"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("500: %v", err)
	}
	if _, err := c.Get(ctx, Ref{"ReelDrop", "NOVALUE"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("no value: %v", err)
	}
	if _, err := New(srv.URL, "").Get(ctx, Ref{"ReelDrop", "PLR_TOKEN"}); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("no token: %v", err)
	}
	if _, err := New("http://127.0.0.1:1", "tok").Get(ctx, Ref{"a", "b"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unreachable: %v", err)
	}
}

func TestListCacheAndExists(t *testing.T) {
	srv, lists := server(t)
	c := New(srv.URL, "tok")
	ctx := context.Background()
	keys, err := c.List(ctx, false)
	if err != nil || len(keys) != 2 || keys[0] != "ReelDrop/PLR_TOKEN" || keys[1] != "Root/ROOTKEY" {
		t.Fatalf("list: %v %v", keys, err)
	}
	_, _ = c.List(ctx, false)
	if *lists != 1 {
		t.Fatalf("cache not used: %d fetches", *lists)
	}
	ok, err := c.Exists(ctx, Ref{"ReelDrop", "NEW"})
	if err != nil || ok || *lists != 2 {
		t.Fatalf("miss must refetch once: ok=%v err=%v fetches=%d", ok, err, *lists)
	}
}
