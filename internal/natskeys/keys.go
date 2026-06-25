package natskeys

import "github.com/midtxwn/geotruth/pkg/natskeys"

var StreamSubjects = []string{"pos.>", "cmd.>"}

const (
	StreamName            = natskeys.StreamName
	DurableGeoTruth       = "geotruth"
	SubjectCmdObjRegister = "cmd.object.register"
	SubjectCmdObjRemove   = "cmd.object.remove"
	SubjectPosRaw         = "pos.raw"
)

func SubjectPosRawObject(objectID string) string {
	return SubjectPosRaw + "." + objectID
}
