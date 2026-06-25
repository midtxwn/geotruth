package natspublish

const (
	KVAreas = "areas"
)

// Object ingress subjects are per-object so the ingester can route every
// command for one object to the same ordered publish shard.
const (
	IngesterObjectPrefix   = "ingester.object."
	IngesterObjectWildcard = "ingester.object.*.*"

	IngesterOpRegister     = "register"
	IngesterOpRemove       = "remove"
	IngesterOpPosition     = "position"
	IngesterOpPositionSync = "position_sync"
)

const (
	AreaRegister = "area.register"
	AreaRemove   = "area.remove"
)

func IngesterRegisterObjectSubject(objectID string) string {
	return ingesterObjectSubject(objectID, IngesterOpRegister)
}

func IngesterRemoveObjectSubject(objectID string) string {
	return ingesterObjectSubject(objectID, IngesterOpRemove)
}

func IngesterUpdatePositionSubject(objectID string) string {
	return ingesterObjectSubject(objectID, IngesterOpPosition)
}

func IngesterUpdatePositionSyncSubject(objectID string) string {
	return ingesterObjectSubject(objectID, IngesterOpPositionSync)
}

func ingesterObjectSubject(objectID, op string) string {
	return IngesterObjectPrefix + objectID + "." + op
}
