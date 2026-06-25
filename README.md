# GeoTruth

GeoTruth is a Go spatial tracking service for local Cartesian environments that
need recoverable spatial events. It tracks object state, detects transitions
through polygonal areas, answers spatial queries, and uses NATS JetStream to
persist events so listeners can recover after crashes or disconnects.

The project is designed for local Cartesian spaces such as buildings,
facilities, simulations, or operational areas where objects move through named
regions and polygonal areas.

This repository publishes the code developed and discussed as part of a final
degree thesis project.

## Why GeoTruth

- Event-driven API over NATS: object writes, queries, and public events use NATS
  subjects.
- Persistent event history: object commits and derived geofence events are stored
  in the `GT_EVENTS` JetStream stream.
- Recoverable listeners: clients can consume public events and replay missed
  messages after a crash or disconnect.
- HA-ready persistence: the persistent backend is NATS JetStream, which can be
  deployed with replicas on a mature NATS Server cluster.
- Region-based processing: a caller-provided resolver assigns objects to
  regions, and GeoTruth fans spatial work out to per-region workers.
- Processed write acknowledgements: object writes return a `CommitAck` only
  after GeoTruth has processed the command and committed the public event.
- Operational compaction: old stream messages can be removed without deleting
  the latest recoverable state for active objects.

GeoTruth fits best when the important requirement is not just "where is this
object now?", but "which spatial events happened, were they persisted, and can
listeners recover them?"

## GeoTruth and Tile38

Tile38 deserves credit as a mature, high-performance open-source geospatial
server, and it was the external reference used in the thesis evaluation.

GeoTruth is a strong fit when the workload needs local 2D tracking, explicit
region routing, persistent spatial events, listener recovery, and an event-driven
API. In the thesis local-monitoring benchmarks, GeoTruth outperformed Tile38 in
the eventful scenarios that matter most for this project: periodic geofence
crossings, forced geofence transitions, and logical region transitions. It also
uses NATS JetStream as the persistent backend, so deployments can rely on a
mature NATS Server cluster with configurable replicas when high availability is
required.

## Public branches

- `main`: current usable GeoTruth implementation with compact public event JSON.
- `spatial`: historical Ingester + SPATIAL two-hop architecture used for
  architectural comparison.
- `single-stream-no-compact-json`: single-service GeoTruth architecture before
  compact public event JSON.

The `main` branch is intended for normal use. The historical branches are kept
for reproducibility and thesis traceability.

## Architecture

GeoTruth exposes object write subjects, area write subjects, query subjects, and
public event streams over NATS. Object writes are processed by GeoTruth and
committed to the `GT_EVENTS` JetStream stream. Areas are stored in the `areas`
key-value bucket. Event listeners consume public object and geofence events from
`GT_EVENTS`.

The system uses local Cartesian coordinates. Spatial calculations are 2D; `Z`
is carried as object metadata and used by the configured region resolver to
choose the logical region. Deployments with a real topology should provide their
own `regionresolver.Resolver`.

See [docs/architecture.md](docs/architecture.md) for the public architecture
notes and invariants.

## Limits

- Coordinates are local Cartesian values, not latitude/longitude.
- Geometry is 2D. Object dimensions are width and height only.
- GeoTruth evaluates object-to-area transitions; it is not an object collision
  engine.
- The standalone `cmd/geotruth` runner uses a simple flat resolver for local
  smoke tests. Production integrations should provide their own resolver through
  the embedded runtime.
- Production writers are expected to coordinate ownership of each object ID.
  GeoTruth serializes commands once they enter the service, but it does not
  coordinate multiple external writers for the same object.

## Build

Use the Go version declared in `go.mod`.

```bash
go build ./...
go test ./...
go vet ./...
```

A standalone debug-oriented local runner is available under `cmd/geotruth`.
Library-style integrations should prefer the runtime helpers in `embedded`,
which allow callers to provide their own NATS connector and region resolver.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
