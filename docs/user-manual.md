# User Manual

This manual is for applications and operators that use GeoTruth as a service. It explains how to start GeoTruth, publish areas and objects, query state, read events, and interpret errors without reading the technical manual.

GeoTruth maintains processed spatial truth in local Cartesian coordinates. It can register areas, register moving objects, update positions, remove objects, answer spatial queries, and emit position or geofence events. A geofence, or geofencing event, is an enter or exit event produced by the geometric relationship between an object and an area.

## Minimum Concepts

GeoTruth is used through NATS. Writes and queries use request/reply, the NATS request/response pattern: the client sends JSON to a subject and receives one response.

JetStream is the persistent layer of NATS. GeoTruth uses it to:

- Store public events in the `GT_EVENTS` stream.
- Store areas in the `areas` Key/Value bucket.
- Let durable consumers recover events after disconnecting.

The system uses logical regions. An object is compared only with areas in the same region. The local runner included in the repository uses one region, `"0"`. A real deployment can inject its own region resolver.

## Quick Local Startup

Start NATS with JetStream:

```bash
nats-server -js -sd ./nats-data
```

In another terminal, start GeoTruth:

```bash
export NATS_URL=nats://127.0.0.1:4222
go run ./cmd/geotruth
```

If `NATS_URL` is not set, the default NATS URL is used. This local mode is enough to check the API with region `"0"`.

## Quick Flow With NATS CLI

Register an area:

```bash
nats request area.register '{
  "id": "zone-a",
  "region": "0",
  "points": [
    {"x": 0, "y": 0},
    {"x": 4, "y": 0},
    {"x": 4, "y": 3},
    {"x": 0, "y": 3}
  ]
}'
```

Register an object:

```bash
nats request geotruth.object.obj1.register '{
  "id": "obj1",
  "dims": {"width": 0.8, "height": 0.5}
}'
```

Update its position:

```bash
nats request geotruth.object.obj1.position '{
  "id": "obj1",
  "x": 1,
  "y": 1,
  "z": 0,
  "rot_y": 0
}'
```

Query nearby objects:

```bash
nats request query.nearby '{
  "region": "0",
  "x": 1,
  "y": 1,
  "radius_meters": 5
}'
```

Watch events:

```bash
nats sub 'gt.events.v1.>'
```

Remove the object and area:

```bash
nats request geotruth.object.obj1.remove '{"id":"obj1"}'
nats request area.remove '{"id":"zone-a"}'
```

The simple `nats sub` command is useful for manual inspection. Real applications should use durable JetStream consumers when they need to recover missed events.

## Responses And Errors

All public APIs return a common JSON envelope.

Successful response without data:

```json
{"ok":true}
```

Successful response with data:

```json
{"ok":true,"data":{}}
```

Error response:

```json
{"ok":false,"error":"problem description","error_code":"optional-code"}
```

`error_code` may be omitted. When present, it is more stable for client logic than the human-readable `error` text.

Object writes return a `CommitAck`:

```json
{"ok":true,"data":{"instance_id":"b1i1","commit_seq":1}}
```

A successful object write response means GeoTruth has processed the command and confirmed the public event in `GT_EVENTS`.

## Public Writes

Subjects:

```text
area.register
area.remove
geotruth.object.{objectID}.register
geotruth.object.{objectID}.position
geotruth.object.{objectID}.remove
```

Object and area IDs must not contain `.`. For object writes, the ID in the subject must match the JSON `id` field.

Payload for `area.register`:

```json
{
  "id": "zone-a",
  "region": "0",
  "points": [
    {"x": 0, "y": 0},
    {"x": 4, "y": 0},
    {"x": 4, "y": 3},
    {"x": 0, "y": 3}
  ]
}
```

Payload for `area.remove`:

```json
{"id":"zone-a"}
```

Payload for `geotruth.object.{objectID}.register`:

```json
{
  "id": "obj1",
  "dims": {"width": 0.8, "height": 0.5},
  "client_op_id": "optional"
}
```

Payload for `geotruth.object.{objectID}.position`:

```json
{
  "id": "obj1",
  "x": 1,
  "y": 1,
  "z": 0,
  "rot_y": 0,
  "client_op_id": "optional"
}
```

Payload for `geotruth.object.{objectID}.remove`:

```json
{
  "id": "obj1",
  "client_op_id": "optional"
}
```

`client_op_id` is optional and is propagated to public object events. It is useful for correlating a client operation with the events emitted by GeoTruth.

## Public Queries

Queries also use request/reply. Available subjects:

```text
query.nearby
query.nearby_of
query.within_area
query.areas_containing_object
query.areas_at_point
query.intersecting_objects
query.object_bounds
query.nearby_areas
query.area
query.all_objects
query.all_objects_oriented
query.object_data
query.all_areas
query.region_of
query.region_from_point
```

Common request bodies:

```json
{"region":"0","x":1,"y":1,"radius_meters":5}
```

for `query.nearby` and `query.nearby_areas`.

```json
{"object_id":"obj1","radius_meters":5}
```

for `query.nearby_of`.

```json
{"region":"0","area_id":"zone-a"}
```

for `query.within_area`.

```json
{"object_id":"obj1"}
```

for `query.areas_containing_object`, `query.intersecting_objects`, `query.object_bounds`, `query.object_data`, and `query.region_of`.

```json
{"area_id":"zone-a"}
```

for `query.area`.

```json
{"x":1,"y":1,"z":0}
```

for `query.region_from_point`.

Queries that accept an optional filter use `regex`:

```json
{"region":"0","x":1,"y":1,"radius_meters":5,"regex":"^sensor-"}
```

If the regular expression is invalid, GeoTruth returns an error response.

## Public Events

All public events are published under `gt.events.v1.>`.

Main subjects:

```text
gt.events.v1.object.{id}.registered
gt.events.v1.object.{id}.position.updated
gt.events.v1.object.{id}.removed
gt.events.v1.object.{id}.geofence.{areaID}.entered
gt.events.v1.object.{id}.geofence.{areaID}.exited
gt.events.v1.geotruth.booted
```

Useful watch patterns:

```text
gt.events.v1.>
gt.events.v1.object.{id}.>
gt.events.v1.object.{id}.geofence.>
gt.events.v1.object.*.geofence.{areaID}.>
```

Common compact fields:

- `e`: deterministic event ID.
- `k`: event type.
- `t`: event time.
- `o`: object ID.
- `i`: instance ID.
- `s`: commit sequence.
- `r`: region.
- `p`: position.
- `d`: dimensions.
- `a`: areas currently containing the object.
- `g`: area ID for geofence events.
- `c`: `client_op_id`, when the client sent one.

Go clients do not need to decode these fields manually. They can use the public types in `pkg/natswatch`.

## Go Client Usage

Basic connection and clients:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

nc, err := nats.Connect(
    "nats://127.0.0.1:4222",
    nats.RetryOnFailedConnect(true),
    nats.MaxReconnects(-1),
    nats.ReconnectWait(2*time.Second),
)
if err != nil {
    return err
}
defer nc.Drain()

pub := natspublish.New(nc)
query := natsquery.New(nc)
```

Register an area:

```go
points := []domain.Point{
    {X: 0, Y: 0},
    {X: 4, Y: 0},
    {X: 4, Y: 3},
    {X: 0, Y: 3},
}

if err := pub.RegisterArea(ctx, "zone-a", "0", points); err != nil {
    return err
}
```

Register and move an object:

```go
dims := domain.ObjectDimensions{Width: 0.8, Height: 0.5}

ack, err := pub.RegisterObject(ctx, "obj1", dims)
if err != nil {
    return err
}

fmt.Println(ack.InstanceID, ack.CommitSeq)

ack, err = pub.UpdateObjectPosition(ctx, "obj1", 1, 1, 0, 0)
if err != nil {
    return err
}
```

Query:

```go
nearby, err := query.NearbyObjects(ctx, "0", 1, 1, 5, nil)
if err != nil {
    return err
}

fmt.Println(len(nearby))
```

Consume persistent events:

```go
js, err := jetstream.New(nc)
if err != nil {
    return err
}

cons, err := natsconsumer.New(
    js,
    natskeys.GTStreamName,
    natsconsumer.Config{
        Name:          "demo-client",
        DeliverPolicy: jetstream.DeliverNewPolicy,
    },
)
if err != nil {
    return err
}
defer cons.Close()

events, unsubscribe, err := natswatch.WatchObject(ctx, cons, "obj1")
if err != nil {
    return err
}
defer unsubscribe()

for ev := range events {
    if ev.PositionUpdated != nil {
        fmt.Println(ev.PositionUpdated.Position)
    }
}
```

## Embedded Deployment

For Go integrations, the recommended entrypoint is `embedded.RunGeoTruth`. The application provides a NATS connector and a region resolver:

```go
connectNATS := func(role string) (*nats.Conn, error) {
    return nats.Connect(
        "nats://127.0.0.1:4222",
        nats.Name("geotruth-"+role),
        nats.RetryOnFailedConnect(true),
        nats.MaxReconnects(-1),
        nats.ReconnectWait(2*time.Second),
    )
}

cfg := geotruth.Config{
    Storage:  jetstream.FileStorage,
    Replicas: 1,
}

svc, err := embedded.RunGeoTruth(ctx, cfg, geotruth.Dependencies{
    NATS:     connectNATS,
    Resolver: resolver,
})
if err != nil {
    return err
}

<-svc.Ready()
```

The NATS connector is where the deployment injects TLS, credentials, permissions, URL, client names, and reconnect policy. GeoTruth assumes clients that can publish or subscribe to its subjects have already been authorized by NATS.

## Basic Operations

Show stats:

```bash
export NATS_URL=nats://127.0.0.1:4222
go run ./cmd/geotruth-admin stats
```

Plan compaction without deleting:

```bash
go run ./cmd/geotruth-admin compact
```

Execute compaction with NDJSON export first:

```bash
go run ./cmd/geotruth-admin compact --execute --export-dir ./exports
```

Do not share operational credentials with normal publishers. A sensor that only publishes positions does not need permission to register areas, read all events, or compact the stream.

## Common Errors

- `nats request: timeout`: GeoTruth is not running, the subject is wrong, or NATS is not reachable.
- ID mismatch: the ID in the subject does not match the JSON `id` field.
- Invalid ID: the object or area ID contains `.`.
- Register object error: the object is already active.
- Move object error: the object is not registered or has been removed.
- Regex error: a query contains an invalid regular expression.
- No events after client restart: use a durable JetStream consumer instead of a simple Core NATS subscription.
