# GeoTruth - single-stream-no-compact-json branch

This branch is a historical snapshot of the single-service GeoTruth architecture
before compact public event JSON. It is kept for reproducibility and
architectural traceability, not as the recommended version for new integrations.

For normal use, see the `main` branch.

## Branch role

The `single-stream-no-compact-json` branch preserves the design where GeoTruth
already acts as the direct processed write gateway and commits public events to
`GT_EVENTS`, but before the final compact JSON event format used by `main`.

This snapshot is useful for comparing the single-stream architecture against
the final compact-event implementation.

## Public branches

- `main`: current usable GeoTruth implementation with compact public event JSON.
- `spatial`: historical Ingester + SPATIAL two-hop architecture used for
  architectural comparison.
- `single-stream-no-compact-json`: this historical single-service snapshot
  before compact public event JSON.

## Build

Use the Go version declared in `go.mod`.

```bash
go build ./...
go test ./...
go vet ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
