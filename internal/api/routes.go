package api

import (
	"net/http"
	"slices"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

type authKind int

const (
	authNone authKind = iota
	authBearer
	authRunToken
)

// route is one entry of the single table that feeds both the mux and the
// OpenAPI parity test.
type route struct {
	method  string
	pattern string
	auth    authKind
	scope   string
	codes   []int // documented status codes (the parity test compares them)
	static  bool  // dashboard asset, not part of the API document
	handler http.HandlerFunc
}

func (s *Server) routeTable() []route {
	c := func(codes ...int) []int { return codes }
	return withMiddlewareCodes([]route{
		{"GET", "/healthz", authNone, "", c(200, 503), false, s.healthz},
		{"GET", "/v1/openapi.json", authNone, "", c(200), false, s.openapi},
		{"GET", "/v1/status", authBearer, "", c(200, 401), false, s.status},

		{"GET", "/v1/jobs", authBearer, model.ScopeJobsRead, c(200, 400, 401, 403), false, s.listJobs},
		{"POST", "/v1/jobs", authBearer, model.ScopeJobsWrite, c(201, 400, 401, 403, 409, 413, 503), false, s.createJob},
		{"POST", "/v1/jobs/validate", authBearer, model.ScopeJobsWrite, c(200, 400, 401, 403, 413), false, s.validateNewJob},
		{"GET", "/v1/jobs/{id}", authBearer, model.ScopeJobsRead, c(200, 401, 403, 404), false, s.getJob},
		{"PATCH", "/v1/jobs/{id}", authBearer, model.ScopeJobsWrite, c(200, 400, 401, 403, 404, 409, 413, 503), false, s.patchJob},
		{"DELETE", "/v1/jobs/{id}", authBearer, model.ScopeJobsWrite, c(204, 401, 403, 404), false, s.deleteJob},
		{"POST", "/v1/jobs/{id}/invoke", authBearer, model.ScopeRunsInvoke, c(200, 202, 400, 401, 403, 404, 409, 413, 503), false, s.invokeJob},
		{"POST", "/v1/jobs/{id}/enable", authBearer, model.ScopeJobsWrite, c(200, 401, 403, 404, 409), false, s.enableJob},
		{"POST", "/v1/jobs/{id}/disable", authBearer, model.ScopeJobsWrite, c(200, 401, 403, 404), false, s.disableJob},
		{"POST", "/v1/jobs/{id}/validate", authBearer, model.ScopeJobsRead, c(200, 401, 403, 404), false, s.validateJob},

		{"GET", "/v1/runs", authBearer, model.ScopeJobsRead, c(200, 400, 401, 403), false, s.listRuns},
		{"POST", "/v1/runs", authBearer, model.ScopeRunsAdhoc, c(200, 202, 400, 401, 403, 409, 413, 503), false, s.createAdhoc},
		{"GET", "/v1/runs/{id}", authBearer, model.ScopeJobsRead, c(200, 401, 403, 404), false, s.getRun},
		{"GET", "/v1/runs/{id}/logs", authBearer, model.ScopeJobsRead, c(200, 400, 401, 403, 404), false, s.runLogs},
		{"POST", "/v1/runs/{id}/cancel", authBearer, model.ScopeRunsInvoke, c(200, 202, 401, 403, 404, 409), false, s.cancelRun},
		{"POST", "/v1/runs/{id}/heartbeat", authRunToken, "", c(200, 401, 409), false, s.heartbeat},
		{"POST", "/v1/runs/{id}/complete", authRunToken, "", c(200, 202, 400, 401, 409, 413), false, s.complete},

		{"GET", "/v1/clients", authBearer, model.ScopeAdmin, c(200, 401, 403), false, s.listClients},
		{"POST", "/v1/clients", authBearer, model.ScopeAdmin, c(201, 400, 401, 403, 409, 413), false, s.createClient},
		{"PATCH", "/v1/clients/{id}", authBearer, model.ScopeAdmin, c(200, 400, 401, 403, 404, 413), false, s.patchClient},
		{"DELETE", "/v1/clients/{id}", authBearer, model.ScopeAdmin, c(204, 401, 403, 404, 409), false, s.deleteClient},
		{"POST", "/v1/clients/{id}/rotate", authBearer, model.ScopeAdmin, c(200, 400, 401, 403, 404, 413), false, s.rotateClient},

		{"GET", "/v1/secrets", authBearer, model.ScopeJobsWrite, c(200, 401, 403, 503), false, s.listSecrets},

		{"GET", "/{$}", authNone, "", c(200), true, s.static},
		{"GET", "/app.js", authNone, "", c(200), true, s.static},
		{"GET", "/style.css", authNone, "", c(200), true, s.static},
	})
}

// withMiddlewareCodes adds what wrap itself can answer on authenticated
// routes: 429 (failed-attempt lockout) and 503 (token or engine unavailable).
func withMiddlewareCodes(routes []route) []route {
	for i, rt := range routes {
		if rt.auth == authNone {
			continue
		}
		for _, code := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
			if !slices.Contains(rt.codes, code) {
				routes[i].codes = append(routes[i].codes, code)
			}
		}
	}
	return routes
}

func (s *Server) static(w http.ResponseWriter, r *http.Request) { s.d.Static.ServeHTTP(w, r) }
