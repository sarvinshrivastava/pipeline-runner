// Package metrics defines the Prometheus collectors. The fire-delay histogram
// is the precision proof: it measures how late each run left the engine.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every collector on its own registry.
type Metrics struct {
	Registry *prometheus.Registry

	FireDelay        *prometheus.HistogramVec
	RunsTotal        *prometheus.CounterVec
	RunDuration      *prometheus.HistogramVec
	Late             *prometheus.HistogramVec
	PendingRuns      prometheus.Gauge
	Inflight         *prometheus.GaugeVec
	QueueDepth       *prometheus.GaugeVec
	WaitingRuns      prometheus.Gauge
	SkippedTotal     *prometheus.CounterVec
	UnflushedWrites  prometheus.Gauge
	EngineLoopAgeSec prometheus.GaugeFunc
	// Coalesced counts cron occurrences that never got a run: reason
	// "backlog" (collapsed inside max_late) or "expired" (older than max_late).
	Coalesced *prometheus.CounterVec

	seriesMu sync.Mutex
	series   map[[2]string]struct{} // {job, client} pairs with a Coalesced series
}

// New builds the collectors. loopAge reports the engine loop age in seconds.
func New(loopAge func() float64) *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		FireDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "plr_fire_delay_seconds",
			Help:    "Delay between a run's scheduled instant and its dispatch.",
			Buckets: []float64{.001, .002, .005, .01, .02, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"target"}),
		RunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "plr_runs_total", Help: "Runs reaching a terminal status.",
		}, []string{"job", "client", "status"}),
		RunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "plr_run_duration_seconds", Help: "Run duration from dispatch to terminal status.",
			Buckets: prometheus.ExponentialBuckets(0.01, 4, 10),
		}, []string{"job", "client"}),
		Late: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "plr_late_seconds", Help: "Lateness at dispatch.",
			Buckets: []float64{0.01, 0.1, 1, 5, 30, 60, 300, 900},
		}, []string{"job"}),
		PendingRuns:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "plr_pending_runs", Help: "Pending runs known to the engine."}),
		Inflight:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "plr_inflight", Help: "Runs holding a pool slot."}, []string{"target"}),
		QueueDepth:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "plr_queue_depth", Help: "Due runs parked on the pool list, waiting for a slot or for their client's share of slots."}, []string{"target"}),
		WaitingRuns:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "plr_waiting_runs", Help: "Async runs waiting for completion."}),
		SkippedTotal:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "plr_skipped_total", Help: "Runs skipped by overlap=skip."}, []string{"job"}),
		UnflushedWrites: prometheus.NewGauge(prometheus.GaugeOpts{Name: "plr_unflushed_writes", Help: "Terminal results held in memory after a DB write failure."}),
		EngineLoopAgeSec: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "plr_engine_loop_age_seconds", Help: "Seconds since the engine loop last completed an iteration.",
		}, loopAge),
		Coalesced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "plr_coalesced_occurrences_total", Help: "Cron occurrences that did not run: folded into a later run (reason=backlog) or past max_late_sec (reason=expired).",
		}, []string{"job", "client", "reason"}),
		series: map[[2]string]struct{}{},
	}
	m.Registry.MustRegister(m.FireDelay, m.RunsTotal, m.RunDuration, m.Late, m.PendingRuns, m.Inflight, m.QueueDepth,
		m.WaitingRuns, m.SkippedTotal, m.UnflushedWrites, m.EngineLoopAgeSec, m.Coalesced,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// AddCoalesced adds n to a job's coalesced series. Safe from any goroutine.
func (m *Metrics) AddCoalesced(job, client, reason string, n int) {
	if n <= 0 {
		return
	}
	m.seriesMu.Lock()
	defer m.seriesMu.Unlock()
	m.series[[2]string{job, client}] = struct{}{}
	m.Coalesced.WithLabelValues(job, client, reason).Add(float64(n))
}

// DeleteCoalesced drops every series of one job.
func (m *Metrics) DeleteCoalesced(job, client string) {
	m.seriesMu.Lock()
	defer m.seriesMu.Unlock()
	m.deleteCoalescedLocked([2]string{job, client})
}

// SweepCoalesced drops the series of every job for which live is false.
func (m *Metrics) SweepCoalesced(live func(job, client string) bool) {
	m.seriesMu.Lock()
	defer m.seriesMu.Unlock()
	for k := range m.series {
		if !live(k[0], k[1]) {
			m.deleteCoalescedLocked(k)
		}
	}
}

func (m *Metrics) deleteCoalescedLocked(k [2]string) {
	delete(m.series, k)
	m.Coalesced.DeletePartialMatch(prometheus.Labels{"job": k[0], "client": k[1]})
}

// Handler serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
