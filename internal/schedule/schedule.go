// Package schedule computes cron fire times with explicit DST handling.
//
// robfig/cron is used only to parse. Its SpecSchedule.Next skips a
// spring-forward fixed-time job and fires a fall-back one twice, and Go's
// time.Date resolves DST edges inconsistently between zones. See
// learnings/robfig-cron-dst-skips-spring-forward-and-doubles-fall-back.md and
// learnings/go-time-date-gap-normalises-backwards-and-ambiguity-is-unspecified.md.
//
// Rules:
//   - Expressions whose hour field is a fixed list ("wall-clock" jobs) fire
//     once per matching local time: a time inside a spring-forward gap fires at
//     the end of the gap, and a time inside a fall-back repeat fires on its
//     first occurrence only.
//   - Every other expression follows absolute instants.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// scanYears bounds every search so an impossible expression ends.
const scanYears = 5

var parser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Schedule is a parsed cron expression bound to a location.
type Schedule struct {
	sched     cron.Schedule
	spec      *cron.SpecSchedule // nil for @every
	loc       *time.Location
	wallClock bool
}

// Parse parses expr and binds it to the IANA zone tz ("" means UTC).
func Parse(expr, tz string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return Schedule{}, errors.New("cron expression is empty")
	}
	upper := strings.ToUpper(expr)
	if strings.HasPrefix(upper, "TZ=") || strings.HasPrefix(upper, "CRON_TZ=") {
		return Schedule{}, errors.New("set the timezone with the timezone field, not a TZ= prefix")
	}
	loc, err := LoadLocation(tz)
	if err != nil {
		return Schedule{}, err
	}
	sched, err := parser.Parse(expr)
	if err != nil {
		return Schedule{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	s := Schedule{sched: sched, loc: loc}
	if spec, ok := sched.(*cron.SpecSchedule); ok {
		spec.Location = loc
		s.spec = spec
	}
	s.wallClock = classify(expr)
	return s, nil
}

// LoadLocation loads an IANA zone; "" means UTC. "Local" is refused because it
// depends on the host.
func LoadLocation(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	if tz == "Local" {
		return nil, errors.New("timezone must be an IANA name, not Local")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("unknown timezone %q", tz)
	}
	return loc, nil
}

// classify reports whether the hour field is a fixed list of wall-clock
// hours. Only "every hour" follows absolute instants; ranges and steps
// (9-17/2, */3) name fixed local hours.
func classify(expr string) (wallClock bool) {
	if strings.HasPrefix(expr, "@") {
		switch strings.ToLower(expr) {
		case "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight":
			return true
		}
		return false // @hourly and @every follow absolute instants
	}
	fields := strings.Fields(expr)
	hourIdx := 1
	if len(fields) == 6 {
		hourIdx = 2
	}
	switch fields[hourIdx] {
	case "*", "?", "*/1":
		return false
	}
	return true
}

// Next returns the first fire instant strictly after `after`, in UTC, or the
// zero time when there is none within the scan bound.
func (s Schedule) Next(after time.Time) time.Time {
	if !s.wallClock || s.spec == nil {
		t := s.sched.Next(after.In(s.loc))
		if t.IsZero() {
			return t
		}
		return t.UTC()
	}
	return s.nextWallClock(after)
}

// NextN returns up to n successive fire instants after `after`.
func (s Schedule) NextN(after time.Time, n int) []time.Time {
	out := make([]time.Time, 0, n)
	for len(out) < n {
		t := s.Next(after)
		if t.IsZero() {
			break
		}
		out = append(out, t)
		after = t
	}
	return out
}

// MinGap is the smallest gap between the next n fires. It is zero when fewer
// than two fires exist.
func (s Schedule) MinGap(after time.Time, n int) time.Duration {
	fires := s.NextN(after, n)
	var gap time.Duration
	for i := 1; i < len(fires); i++ {
		d := fires[i].Sub(fires[i-1])
		if gap == 0 || d < gap {
			gap = d
		}
	}
	return gap
}

const starBit = 1 << 63

func (s Schedule) nextWallClock(after time.Time) time.Time {
	spec := s.spec
	local := after.In(s.loc)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
	end := day.AddDate(scanYears, 0, 0)
	for ; day.Before(end); day = day.AddDate(0, 0, 1) {
		if spec.Month&(1<<uint(day.Month())) == 0 || !dayMatches(spec, day) {
			continue
		}
		for h := 0; h < 24; h++ {
			if spec.Hour&(1<<uint(h)) == 0 {
				continue
			}
			for m := 0; m < 60; m++ {
				if spec.Minute&(1<<uint(m)) == 0 {
					continue
				}
				for sec := 0; sec < 60; sec++ {
					if spec.Second&(1<<uint(sec)) == 0 {
						continue
					}
					if t := resolve(s.loc, day.Year(), day.Month(), day.Day(), h, m, sec); t.After(after) {
						return t.UTC()
					}
				}
			}
		}
	}
	return time.Time{}
}

// dayMatches copies robfig's semantics: when either day field is a star, both
// must match; otherwise either may match.
func dayMatches(spec *cron.SpecSchedule, day time.Time) bool {
	dom := spec.Dom&(1<<uint(day.Day())) > 0
	dow := spec.Dow&(1<<uint(day.Weekday())) > 0
	if spec.Dom&starBit > 0 || spec.Dow&starBit > 0 {
		return dom && dow
	}
	return dom || dow
}

// resolve maps a local wall time to an instant. In a fall-back repeat it
// returns the earlier instant; in a spring-forward gap it returns the end of
// the gap.
func resolve(loc *time.Location, y int, mo time.Month, d, h, mi, sec int) time.Time {
	naive := time.Date(y, mo, d, h, mi, sec, 0, time.UTC)
	_, offBefore := naive.Add(-26 * time.Hour).In(loc).Zone()
	_, offAfter := naive.Add(26 * time.Hour).In(loc).Zone()

	var best time.Time
	for _, off := range []int{offBefore, offAfter} {
		t := naive.Add(-time.Duration(off) * time.Second)
		lt := t.In(loc)
		if lt.Year() == y && lt.Month() == mo && lt.Day() == d && lt.Hour() == h && lt.Minute() == mi && lt.Second() == sec {
			if best.IsZero() || t.Before(best) {
				best = t
			}
		}
	}
	if !best.IsZero() {
		return best
	}
	// Gap: naive − offBefore lands after the transition, so the zone's start
	// is the transition instant.
	start, _ := naive.Add(-time.Duration(offBefore) * time.Second).In(loc).ZoneBounds()
	return start
}
