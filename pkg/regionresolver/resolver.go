package regionresolver

// NoPrevRegion is passed to Resolve when the object has no prior region
// assignment (initial placement). It is the nil *string value.
var NoPrevRegion *string = nil

// Resolver assigns a spatial point to a region, given the object's previous
// region. The engine calls Resolve on every position update; the returned
// region ID must be present in KnownRegions().
//
// Implementations may use prevRegion to implement state-dependent logic
// such as hysteresis. When prevRegion is nil, the object has no prior region
// and the resolver should perform stateless initial placement.
type Resolver interface {
	// Resolve returns the region ID for a spatial point, given the object's
	// previous region. prevRegion is nil for initial placement (no prior
	// region assignment).
	//
	// Resolve must return a region ID that is present in KnownRegions().
	// The engine validates this at runtime and NAKs messages that resolve
	// to unknown regions.
	//
	// Implementations may return a non-nil error for irrecoverable
	// conditions (e.g., the point falls outside all known regions).
	Resolve(x, y, z float64, prevRegion *string) (regionID string, err error)

	// KnownRegions returns the complete set of region IDs that Resolve can
	// return, in any order. The engine creates one RegionState per known
	// region and one RegionWorker per known region. Lock ordering for
	// cross-region transitions uses lexicographic sort of these IDs.
	//
	// KnownRegions must return at least one region, all IDs must be non-empty
	// and unique. The engine validates this at startup.
	KnownRegions() []string
}
