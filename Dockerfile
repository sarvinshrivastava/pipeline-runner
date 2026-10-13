# Build: a static binary (pure-Go SQLite, no cgo) with tzdata embedded.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG GIT_SHA=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${GIT_SHA}" -o /plr ./cmd/plr

# Run: CA certs for HTTPS callbacks and git (used by container targets in 1b).
FROM alpine:3.22
RUN apk add --no-cache ca-certificates git
COPY --from=build /plr /usr/local/bin/plr
ENV PORT=3000 METRICS_PORT=9464
EXPOSE 3000 9464
ENTRYPOINT ["plr"]
CMD ["serve"]
