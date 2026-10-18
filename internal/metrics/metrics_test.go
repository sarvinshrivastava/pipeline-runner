package metrics

import "testing"

// seriesOf counts the series of one metric family.
func seriesOf(t *testing.T, m *Metrics, name string) int {
	t.Helper()
	fams, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	return 0
}

func TestAddJobPushSeriesSwept(t *testing.T) {
	m := New(func() float64 { return 0 })
	m.AddJobPush("tick", "c1", "ok")
	m.AddCoalesced("tick", "c1", "backlog", 1)
	m.DeleteJobSeries("tick", "c1")
	if n := seriesOf(t, m, "plr_job_push_total") + seriesOf(t, m, "plr_coalesced_occurrences_total"); n != 0 {
		t.Fatalf("%d series left after DeleteJobSeries", n)
	}

	// A push that finishes after the delete re-creates the series; the next
	// sweep removes it and keeps live jobs.
	m.AddJobPush("tick", "c1", "ok")
	m.AddJobPush("live", "c1", "failed")
	m.SweepJobSeries(func(job, _ string) bool { return job == "live" })
	if n := seriesOf(t, m, "plr_job_push_total"); n != 1 {
		t.Fatalf("%d series after sweep, want 1 (live)", n)
	}
}
