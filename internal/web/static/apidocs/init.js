"use strict";

// guardRequest refuses follow=1 on the logs route: it would open an SSE stream
// for up to 1 h and hold one of the client's 4 stream slots.
function guardRequest(req) {
  const url = new URL(req.url, location.origin);
  const follow = url.searchParams.get("follow");
  if (url.pathname.endsWith("/logs") && (follow === "1" || follow === "true")) {
    throw new Error("follow=1 opens a 1 h SSE stream and is disabled in /docs; use curl -N");
  }
  return req;
}

window.ui = SwaggerUIBundle({
  url: "/v1/openapi.json",
  dom_id: "#swagger-ui",
  layout: "BaseLayout",
  presets: [SwaggerUIBundle.presets.apis],
  deepLinking: true,
  displayOperationId: true,
  filter: true,
  docExpansion: "list",
  defaultModelsExpandDepth: 0,
  tryItOutEnabled: false,
  persistAuthorization: false,
  queryConfigEnabled: false,
  validatorUrl: null,
  // Try it out only for reads. Enabling writes means widening this list AND
  // adding a requestInterceptor guard that fills a blank Idempotency-Key and
  // reuses it on a double Execute, or a double click creates two jobs.
  supportedSubmitMethods: ["get", "head"],
  requestInterceptor: guardRequest,
  // guardRequest throws before anything is sent. Swagger UI then has no
  // mutated request to show and crashes rendering the response, so show the
  // request as built instead (guardRequest never changes it).
  showMutatedRequest: false,
});
