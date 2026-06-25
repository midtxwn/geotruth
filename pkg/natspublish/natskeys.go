package natspublish

const (
	KVAreas = "areas"
)

// Object ingress subjects are per-object so GeoTruth can route every command
// for one object through the same ordered mailbox.
const (
	GeoTruthObjectPrefix   = "geotruth.object."
	GeoTruthObjectWildcard = "geotruth.object.*.*"

	GeoTruthOpRegister = "register"
	GeoTruthOpRemove   = "remove"
	GeoTruthOpPosition = "position"
)

const (
	AreaRegister = "area.register"
	AreaRemove   = "area.remove"
)

func GeoTruthRegisterObjectSubject(objectID string) string {
	return geotruthObjectSubject(objectID, GeoTruthOpRegister)
}

func GeoTruthRemoveObjectSubject(objectID string) string {
	return geotruthObjectSubject(objectID, GeoTruthOpRemove)
}

func GeoTruthUpdatePositionSubject(objectID string) string {
	return geotruthObjectSubject(objectID, GeoTruthOpPosition)
}

func geotruthObjectSubject(objectID, op string) string {
	return GeoTruthObjectPrefix + objectID + "." + op
}
