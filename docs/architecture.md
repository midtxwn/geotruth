# GeoTruth Architecture

This document records the public architecture notes needed to understand and
modify GeoTruth.

## Spatial model

GeoTruth models local Cartesian spaces. `domain.Point`, `domain.Triangle`,
`domain.Area`, `geo.OBB`, and R-tree keys are 2D. `domain.Object` carries `X`,
`Y`, `Z`, and `RotY`, but `Z` is used only by the configured region resolver and
is otherwise passthrough metadata.

Areas are 2D polygons attached to exactly one string region. Objects are routed
to a region, then evaluated against the areas in that region. Objects are not
compared with other objects.

## Runtime shape

GeoTruth is the public write gateway and read/query processor. It exposes:

- object write subjects:
  - `geotruth.object.{objectID}.register`
  - `geotruth.object.{objectID}.position`
  - `geotruth.object.{objectID}.remove`
- area write subjects:
  - `area.register`
  - `area.remove`
- query subjects served from in-memory state;
- public event subjects under `gt.events.v1.>`.

Object writes return a processed commit acknowledgement:

```json
{"ok":true,"data":{"instance_id":"...","commit_seq":1}}
```

Area writes update the `areas` JetStream key-value bucket and return:

```json
{"ok":true}
```

`embedded.RunGeoTruth` is the intended runtime entrypoint for integrations that
provide their own NATS connector and `regionresolver.Resolver`.
`embedded.RunLocalStack` starts embedded NATS plus GeoTruth for integration
tests, local development, and debug tooling.

## Regions and concurrency

A single dispatcher goroutine owns per-object mailboxes. At most one command
per object is in flight through worker and publisher stages. Different objects
can proceed concurrently.

The dispatcher resolves each position to a region and sends spatial work to the
corresponding region worker. Region workers own their in-memory R-tree updates
for that region. The important relationship is one worker per known region,
which lets independent object updates be processed in parallel when they route
to different regions.

Implementations of `regionresolver.Resolver` should add hysteresis around region
boundaries when measurements are noisy. Without hysteresis, an object near a
boundary can rapidly alternate between regions and produce noisy geofence
events.

## Persistent events

The only persistent object event stream is `GT_EVENTS`.

Commit-bearing public object events:

- `gt.events.v1.object.{id}.registered`
- `gt.events.v1.object.{id}.position.updated`
- `gt.events.v1.object.{id}.removed`

Derived geofence projection events:

- `gt.events.v1.object.{id}.geofence.{areaID}.entered`
- `gt.events.v1.object.{id}.geofence.{areaID}.exited`

GeoTruth publishes the public object commit event first. That event is the
persistent checkpoint for the processed object command. Geofence projection
events are published after the commit marker and can be repaired from commit
metadata after restart.

Object identity has two layers:

- `objectID`: logical user ID;
- `instance_id`: one incarnation of that object ID between register and remove.

`commit_seq` starts at 1 per instance and increases by 1 for every committed
object command.

## Recovery and listener behavior

On boot, GeoTruth seeds areas from the `areas` KV bucket, recovers latest object
commit events from `GT_EVENTS`, repairs any missing geofence projections that
can be derived from the latest commits, and publishes a boot marker.

Listeners should consume `GT_EVENTS` subjects. Because the stream is persistent,
a listener that crashes or disconnects can resume from its pending messages
instead of relying only on live delivery.

## Commit safety

Region workers update idempotent live state during processing. The
non-idempotent `prevInside` geofence state is deferred until after the commit
envelope is confirmed in `GT_EVENTS`. This keeps retry and redelivery behavior
safe: if publishing fails, the next attempt still compares against the previous
committed geofence snapshot.

## Compaction

`pkg/geotruthops` provides operational helpers for `GT_EVENTS`, including
statistics and compaction.

Compaction protects:

- the latest active commit event for every active object;
- geofence projection event IDs referenced by that latest active commit.

For an object whose latest commit is removed, its older history can be deleted
together because no surviving active commit should resurrect that object on
boot. When an export directory is provided, deleted messages are exported as
NDJSON before removal.

## Package map

- `cmd/geotruth`: standalone local GeoTruth runner.
- `embedded`: runtime helpers and embedded local stack.
- `internal/app/geotruth`: service startup, boot, write handlers, and queries.
- `internal/engine`: dispatcher, per-region workers, and query state.
- `internal/gtevents`: `GT_EVENTS` schemas, builders, publisher, and recovery.
- `internal/geo`: 2D geometry and triangulation.
- `internal/rtree`: R-tree wrappers.
- `pkg/natspublish`: public write client.
- `pkg/natsquery`: public query client.
- `pkg/natswatch`: public event watcher client.
- `pkg/natsconsumer`: owned JetStream pull consumer wrapper.
- `pkg/geotruthops`: stream stats and compaction helpers.
- `pkg/regionresolver`: resolver interface.
