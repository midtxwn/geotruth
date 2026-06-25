# GeoTruth - spatial branch

This branch is a historical snapshot of the Ingester + SPATIAL two-hop
architecture. It is kept for reproducibility and architectural traceability,
not as the recommended version for new integrations.

For normal use, see the `main` branch.

## Branch role

The `spatial` branch preserves the design where object writes were first
accepted by an Ingester service and persisted to an intermediate `SPATIAL`
stream. GeoTruth then consumed `SPATIAL`, updated spatial state, and published
public events to `GT_EVENTS`.

This architecture was useful for comparison, but the final public design in
`main` makes GeoTruth the direct processed write gateway and removes the extra
persistent hop.

## Public branches

- `main`: current usable GeoTruth implementation with compact public event JSON.
- `spatial`: this historical Ingester + SPATIAL two-hop snapshot.
- `single-stream-no-compact-json`: single-service GeoTruth architecture before
  compact public event JSON.

## Build

Use the Go version declared in `go.mod`.

```bash
go build ./...
go test ./...
go vet ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
