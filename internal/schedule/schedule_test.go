package schedule

import (
	"math/rand/v2"
	"testing"
	"time"
)

func mustParse(t *testing.T, expr, tz string) Schedule {
	t.Helper()
	s, err := Parse(expr, tz)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", expr, tz, err)
	}
	return s
}

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func assertFires(t *testing.T, s Schedule, after time.Time, want []string) {
	t.Helper()
	got := s.NextN(after, len(want))
	if len(got) != len(want) {
		t.Fatalf("got %d fires %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if !got[i].Equal(utc(want[i])) {
			t.Fatalf("fire %d = %s, want %s (all: %v)", i, got[i].Format(time.RFC3339), want[i], got)
		}
	}
}

func TestIST7Days(t *testing.T) {
	s := mustParse(t, "30 9 * * *", "Asia/Kolkata")
	var want []string
	for d := 13; d < 20; d++ {
		want = append(want, time.Date(2026, 10, d, 4, 0, 0, 0, time.UTC).Format(time.RFC3339))
	}
	assertFires(t, s, utc("2026-10-13T00:00:00Z"), want)
}

func TestISTHourStepWithUTCAfter(t *testing.T) {
	s := mustParse(t, "0 */2 * * *", "Asia/Kolkata")
	// 00:10Z is 05:40 IST, so the next even IST hour is 06:00 IST = 00:30Z.
	assertFires(t, s, utc("2026-10-13T00:10:00Z"), []string{"2026-10-13T00:30:00Z", "2026-10-13T02:30:00Z"})
}

func TestNYSpringForward(t *testing.T) {
	s := mustParse(t, "30 2 * * *", "America/New_York")
	assertFires(t, s, utc("2026-03-07T12:00:00Z"), []string{
		"2026-03-08T07:00:00Z", // 03:00 EDT: the end of the gap, once
		"2026-03-09T06:30:00Z", // 02:30 EDT
	})
}

func TestNYFallBack(t *testing.T) {
	s := mustParse(t, "30 1 * * *", "America/New_York")
	assertFires(t, s, utc("2026-10-31T12:00:00Z"), []string{
		"2026-11-01T05:30:00Z", // 01:30 EDT, the first occurrence only
		"2026-11-02T06:30:00Z", // 01:30 EST next day
	})
}

func TestBerlinFallBackFiresOnFirstInstant(t *testing.T) {
	s := mustParse(t, "30 2 * * *", "Europe/Berlin")
	assertFires(t, s, utc("2026-10-24T12:00:00Z"), []string{
		"2026-10-25T00:30:00Z", // 02:30 CEST (time.Date alone gives CET)
		"2026-10-26T01:30:00Z",
	})
}

func TestLordHoweHalfHourGapAndRepeat(t *testing.T) {
	gap := mustParse(t, "15 2 * * *", "Australia/Lord_Howe")
	// 2026-10-04 02:00 → 02:30 (+10:30 → +11). 02:15 does not exist.
	assertFires(t, gap, utc("2026-10-03T00:00:00Z"), []string{"2026-10-03T15:30:00Z"})

	rep := mustParse(t, "45 1 * * *", "Australia/Lord_Howe")
	// 2026-04-05 02:00 → 01:30 (+11 → +10:30). 01:45 happens twice.
	assertFires(t, rep, utc("2026-04-04T00:00:00Z"), []string{"2026-04-04T14:45:00Z", "2026-04-05T15:15:00Z"})
}

func TestIntervalThroughFallBack(t *testing.T) {
	s := mustParse(t, "*/15 * * * *", "America/New_York")
	// 01:00 EDT (05:00Z) through 01:45 EST (06:45Z): eight distinct instants.
	assertFires(t, s, utc("2026-11-01T04:59:00Z"), []string{
		"2026-11-01T05:00:00Z", "2026-11-01T05:15:00Z", "2026-11-01T05:30:00Z", "2026-11-01T05:45:00Z",
		"2026-11-01T06:00:00Z", "2026-11-01T06:15:00Z", "2026-11-01T06:30:00Z", "2026-11-01T06:45:00Z",
	})
}

func TestDomDowOrSemantics(t *testing.T) {
	s := mustParse(t, "0 9 1 * MON", "UTC")
	// 2026-10-13 is a Tuesday. Next: Mon 19th, Mon 26th, Sun Nov 1st (dom), Mon Nov 2nd.
	assertFires(t, s, utc("2026-10-13T10:00:00Z"), []string{
		"2026-10-19T09:00:00Z", "2026-10-26T09:00:00Z", "2026-11-01T09:00:00Z", "2026-11-02T09:00:00Z",
	})
}

func TestNeverFires(t *testing.T) {
	s := mustParse(t, "0 0 30 2 *", "UTC")
	if got := s.Next(utc("2026-01-01T00:00:00Z")); !got.IsZero() {
		t.Fatalf("expected zero, got %s", got)
	}
}

func TestEvery(t *testing.T) {
	s := mustParse(t, "@every 90s", "UTC")
	assertFires(t, s, utc("2026-10-13T00:00:00Z"), []string{"2026-10-13T00:01:30Z", "2026-10-13T00:03:00Z"})
	if !s.HasSeconds() {
		t.Fatal("@every must count as sub-minute capable")
	}
}

func TestSixField(t *testing.T) {
	s := mustParse(t, "*/10 * * * * *", "UTC")
	assertFires(t, s, utc("2026-10-13T00:00:00Z"), []string{"2026-10-13T00:00:10Z", "2026-10-13T00:00:20Z"})
	if !s.HasSeconds() || s.MinGap(utc("2026-10-13T00:00:00Z"), 50) != 10*time.Second {
		t.Fatal("six-field schedule misclassified")
	}
}

func TestDescriptorDailyIsWallClock(t *testing.T) {
	s := mustParse(t, "@daily", "Asia/Kolkata")
	assertFires(t, s, utc("2026-10-13T00:00:00Z"), []string{"2026-10-13T18:30:00Z"})
}

func TestParseRejects(t *testing.T) {
	for _, c := range [][2]string{
		{"CRON_TZ=UTC 0 9 * * *", "UTC"},
		{"TZ=UTC 0 9 * * *", "UTC"},
		{"", "UTC"},
		{"61 * * * *", "UTC"},
		{"0 9 * * *", "Mars/Olympus"},
		{"0 9 * * *", "Local"},
	} {
		if _, err := Parse(c[0], c[1]); err == nil {
			t.Errorf("Parse(%q, %q) accepted", c[0], c[1])
		}
	}
}

func TestPropertyStrictlyIncreasing(t *testing.T) {
	exprs := []string{"30 2 * * *", "0 1,2,3 * * *", "*/7 * * * *", "15 */3 * * *", "0 0 * * SUN", "45 1 1 * *", "0 2 * 3,10,11 *"}
	zones := []string{"America/New_York", "Europe/Berlin", "Australia/Lord_Howe", "Asia/Kolkata", "America/Santiago", "UTC"}
	r := rand.New(rand.NewPCG(1, 2))
	for _, e := range exprs {
		for _, z := range zones {
			s := mustParse(t, e, z)
			after := utc("2026-01-01T00:00:00Z").Add(time.Duration(r.IntN(1000)) * time.Hour)
			fires := s.NextN(after, 800)
			for i := 1; i < len(fires); i++ {
				if !fires[i].After(fires[i-1]) {
					t.Fatalf("%s %s: fire %d (%s) not after %s", e, z, i, fires[i], fires[i-1])
				}
			}
		}
	}
}
