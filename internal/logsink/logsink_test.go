package logsink

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHeadTailAndDroppedMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.log")
	s, err := New(path, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []string{"HEAD-12345", "middle-dropped-xxxxx", "TAIL-12345"} {
		if _, err := s.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	got, _ := os.ReadFile(path)
	text := string(got)
	if !strings.HasPrefix(text, "HEAD-12345") || !strings.HasSuffix(text, "TAIL-12345") || !strings.Contains(text, "20 bytes of output dropped") {
		t.Fatalf("log = %q", text)
	}
}

func TestUnderCapKeepsEverything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.log")
	s, _ := New(path, 1024)
	_, _ = s.Write([]byte("hello "))
	_, _ = s.Write([]byte("world"))
	_ = s.Close()
	got, _ := os.ReadFile(path)
	if string(got) != "hello world" {
		t.Fatalf("log = %q", got)
	}
}
