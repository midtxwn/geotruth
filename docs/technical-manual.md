# Technical Manual

This manual is for developers and operators who need to build, modify, test, or diagnose GeoTruth. The user manual explains how to consume the public API; this document explains the internal structure, the invariants that must be preserved, and the normal development and operations procedures.

## Requirements

- The Go version declared in `go.mod`.
- A NATS Server with JetStream enabled.
- Network access from GeoTruth and clients to that NATS Server.
- Standard Go tooling: `go build`, `go test`, `go vet`, and `go mod`.
- Optionally, the NATS CLI for manual API checks.

For a minimal local NATS server:

```bash
nats-server -js -sd ./nats-data
```

GeoTruth does not implement its own authentication or authorization layer. In real deployments, TLS, credentials, accounts, subject permissions, and reconnect policy are configured in NATS and in the connector passed to GeoTruth.

## Build And Run

From the repository root:

```bash
go build ./...
go build ./cmd/geotruth
go build ./cmd/geotruth-admin
go test ./...
go vet ./...
```

The local runner starts GeoTruth against an external NATS server:

```bash
export NATS_URL=nats://127.0.0.1:4222
go run ./cmd/geotruth
```

`cmd/geotruth` uses a minimal resolver: every point belongs to region `"0"`. It is useful for local tests and smoke checks. A deployment with real topology should start GeoTruth from Go with `embedded.RunGeoTruth` and inject its own `regionresolver.Resolver`.

## Repository Structure

- `cmd/geotruth`: local GeoTruth runner.
- `cmd/geotruth-admin`: operational CLI for stats and compaction.
- `embedded`: library startup helpers and an embedded-NATS local stack.
- `internal/app/geotruth`: service startup, boot, write handlers, and queries.
- `internal/engine`: dispatcher, per-object mailboxes, region workers, and in-memory spatial state.
- `internal/gtevents`: `GT_EVENTS` schema, event builders, publisher, and recovery.
- `internal/geo`: 2D geometry, OBBs, SAT checks, triangulation, and polygon validation.
- `internal/rtree`: spatial indexes for objects and areas.
- `internal/streampressure`: stream pressure advisories.
- `pkg/domain`: public domain types.
- `pkg/geotruth`: public embedded-service configuration.
- `pkg/natspublish`: public Go write client.
- `pkg/natsquery`: public Go query client.
- `pkg/natswatch`: public Go event watcher client.
- `pkg/natsconsumer`: JetStream pull consumer wrapper with dynamic filters.
- `pkg/geotruthops`: operational stats and compaction helpers.
- `pkg/messages`: common response envelope helpers.
- `pkg/regionresolver`: public region resolver interface.

The public boundary is `pkg/`, `embedded/`, `cmd/`, and the NATS subjects. Packages under `internal/` are implementation details and should not be used by external clients.

## Runtime Architecture

GeoTruth is the only long-running service in the final runtime. It acts as the public object write gateway, spatial truth processor, query server, event publisher, and area KV watcher.

The main persistent resources are:

- `GT_EVENTS`: the global JetStream stream for public events.
- `areas`: the Key/Value bucket for areas.

Areas are written through the public subjects `area.register` and `area.remove`. GeoTruth watches the `areas` bucket and updates its in-memory state. Queries are served from that in-memory state.

## Spatial Model

GeoTruth works in local Cartesian meters, not latitude/longitude. Interaction geometry is 2D:

- `domain.Point`, `domain.Triangle`, `domain.Area`, `geo.OBB`, and R-tree keys are 2D.
- `domain.Object` carries `X`, `Y`, `Z`, and `RotY`; `Z` is used only by the region resolver.
- Object dimensions are `width` and `height`; there is no vertical extent.
- An area belongs to exactly one logical region.
- Objects and areas in different regions are not compared spatially.

If an area polygon extends outside its declared logical region, GeoTruth does not clip it. Treat that as bad domain configuration rather than an engine failure.

## Public Protocol

Write subjects:

```text
geotruth.object.{objectID}.register
geotruth.object.{objectID}.position
geotruth.object.{objectID}.remove
area.register
area.remove
```

Event subjects:

```text
gt.events.v1.object.{id}.registered
gt.events.v1.object.{id}.position.updated
gt.events.v1.object.{id}.removed
gt.events.v1.object.{id}.geofence.{areaID}.entered
gt.events.v1.object.{id}.geofence.{areaID}.exited
gt.events.v1.geotruth.booted
```

Common watch patterns:

```text
gt.events.v1.>
gt.events.v1.object.{id}.>
gt.events.v1.object.{id}.geofence.>
gt.events.v1.object.*.geofence.{areaID}.>
```

Object and area IDs must not contain `.` because they are embedded in NATS subjects. For object writes, the `objectID` in the subject must match the `id` field in the JSON body.

The response envelope is stable:

```json
{"ok":true}
```

```json
{"ok":true,"data":{}}
```

```json
{"ok":false,"error":"message","error_code":"optional-code"}
```

Object writes return `CommitAck`:

```json
{"ok":true,"data":{"instance_id":"...","commit_seq":1}}
```

A successful object response means GeoTruth processed the command and confirmed the matching public event in `GT_EVENTS`.

## Identity And Events

Object identity has two layers:

- `objectID`: the logical user-provided ID.
- `instance_id`: one concrete lifetime of that object between register and remove.

`commit_seq` starts at 1 for each instance and increases for every confirmed command. Public events carry deterministic IDs in compact field `e`:

```text
gt:p:{objectID}:{instanceID}:{commitSeqBase36}
gt:gf:e:{areaID}:{objectID}:{instanceID}:{commitSeqBase36}
gt:gf:x:{areaID}:{objectID}:{instanceID}:{commitSeqBase36}
gt:o:r:{objectID}:{instanceID}:{commitSeqBase36}
gt:o:d:{objectID}:{instanceID}:{commitSeqBase36}
```

Public payloads use compact keys such as `e`, `k`, `t`, `o`, `i`, `s`, `r`, `p`, `d`, and `a`. Commit events may include `cp`, a compact checkpoint used by GeoTruth recovery. Watch clients do not need to interpret `cp`; they should use the public event types exposed by `pkg/natswatch`.

## Ordering And Concurrency

The main invariants are:

- A single dispatcher owns per-object mailboxes.
- At most one command per `objectID` is in flight.
- Different objects can proceed concurrently.
- Region workers mutate the R-trees for their region.
- Non-idempotent geofence state is committed only after the `GT_EVENTS` commit succeeds.
- The publisher can confirm envelopes for different objects concurrently.
- External producers must coordinate logical ownership of each `objectID`; GeoTruth does not resolve semantic conflicts between multiple writers for the same object.

Do not perform I/O while holding engine locks. Queries should snapshot index data before returning results.

## Recovery

On boot, GeoTruth:

1. Loads areas from the `areas` KV bucket.
2. Recovers the latest commit for each object from `GT_EVENTS`.
3. Reconstructs active in-memory state.
4. Repairs missing geofence projections when they can be derived from commits.
5. Publishes `gt.events.v1.geotruth.booted`.

Object commits are the recoverable source of truth. Geofence events are derived projections. Any change to the data needed for recovery must update checkpoint encoding, recovery code, recovery tests, and compaction rules.

## Embedded Startup

`embedded.RunGeoTruth(ctx, cfg, deps)` starts only GeoTruth. The caller provides:

- `geotruth.NATSConnector`: creates NATS connections for internal roles.
- `regionresolver.Resolver`: decides the region for each point.
- `geotruth.Config`: storage, replicas, stream limits, publisher settings, and pressure monitor settings.

The NATS connector is also the deployment security boundary. It injects URL, TLS, credentials, client names, callbacks, and reconnect policy. GeoTruth drains or closes the connections it receives.

`embedded.RunLocalStack(ctx, cfg, deps)` starts embedded NATS plus GeoTruth. It is intended for tests, local development, and debug tooling, not as the primary production entrypoint.

## Operations

The operational CLI uses `NATS_URL`:

```bash
go run ./cmd/geotruth-admin stats
```

Compaction is planned by default and does not delete anything:

```bash
go run ./cmd/geotruth-admin compact
```

To execute a plan:

```bash
go run ./cmd/geotruth-admin compact --execute --export-dir ./exports
```

Compaction protects:

- The latest active commit for every active object.
- Geofence events referenced by those latest active commits.

For objects whose latest operation is removal, older history can be deleted together because no active commit remains that should reconstruct them on boot. If `--export-dir` is set, deleted messages are exported as NDJSON before removal.

GeoTruth can also publish operational advisories under:

```text
ops.geotruth.v1.stream.GT_EVENTS.pressure
ops.geotruth.v1.stream.GT_EVENTS.publish_failed
```

## Tests And Benchmarks

Basic checks:

```bash
go test ./...
go vet ./...
```

Focused functional test:

```bash
go test ./internal/integration -run TestEndToEnd_ObjectLifecycle -timeout 30s -v
```

Capacity smoke benchmark:

```bash
go test ./internal/integration -run=^$ -bench=^BenchmarkGeoTruthCapacity$ -benchtime=1x -count=1 -timeout=5m -args -objects=10 -hz=1 -sender-workers=2 -pool=2 -warmup=0s -trial-window=1s -drain-timeout=5s -p95-limit=5s -fail-on-capacity=true
```

Supported benchmark scenarios are `position`, `geofence-periodic`, `geofence-toggle`, and `region-transition`.

## Change Rules

- When adding a public subject, update constants, client tests, handlers, and documentation.
- When changing public payloads, preserve compatibility or version the subject namespace.
- When touching recovery, add or update restart tests.
- When touching compaction, prove that active commits and referenced geofence projections remain protected.
- When touching geometry, cover degenerate and concave cases.
- When touching concurrency, verify that one-command-in-flight per object still holds and that no I/O is introduced under locks.
