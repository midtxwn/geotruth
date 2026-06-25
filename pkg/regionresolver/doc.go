// Package regionresolver defines the Resolver interface for assigning spatial
// points to regions. A region is a named partition of 3D space (e.g. "lobby",
// "floor-1", "outside"). Each region maintains its own 2D R-trees and
// geofence state independently.
//
// # Region and Area Relationship
//
// Areas are 2D polygons that belong to exactly one region. An object on region
// R is only tested against areas also on region R. If an area's polygon
// extends beyond its region, the portion outside is not detected by objects on
// other regions. This is a configuration error, not a system error.
//
// # Hysteresis
//
// Implementations that share boundaries between regions should implement
// hysteresis to prevent rapid region oscillation. Without hysteresis, an object
// at a boundary with measurement noise generates oscillating position events
// and, if the region contains any areas, spurious geofence enter/exit events.
// The system does not mandate hysteresis, it is the implementer's
// responsibility.
package regionresolver
