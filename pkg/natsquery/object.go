package natsquery

import (
	"context"

	pubDomain "github.com/midtxwn/geotruth/pkg/domain"
)

type OrientedBounds struct {
	TL, TR, BR, BL pubDomain.Point
}

type Position3D struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type ObjectOriented struct {
	ID       string         `json:"id"`
	Bounds   OrientedBounds `json:"bounds"`
	Position Position3D     `json:"position"`
}

type AllObjectsResp struct {
	Regions map[string][]pubDomain.Object `json:"regions"`
}

type AllObjectsOrientedResp struct {
	Regions map[string][]ObjectOriented `json:"regions"`
}

type RegionOfResp struct {
	Region string `json:"region"`
}

type Object = pubDomain.Object

type AllObjectsReq struct {
	Regex *string `json:"regex,omitempty"`
}

type AllObjectsOrientedReq struct {
	Regex *string `json:"regex,omitempty"`
}

type NearbyReq struct {
	Region       string  `json:"region"`
	X            float64 `json:"x"`
	Y            float64 `json:"y"`
	RadiusMeters float64 `json:"radius_meters"`
	Regex        *string `json:"regex,omitempty"`
}

type NearbyOfReq struct {
	ObjectID     string  `json:"object_id"`
	RadiusMeters float64 `json:"radius_meters"`
	Regex        *string `json:"regex,omitempty"`
}

type WithinAreaReq struct {
	Region string  `json:"region"`
	AreaID string  `json:"area_id"`
	Regex  *string `json:"regex,omitempty"`
}

type IntersectingReq struct {
	ObjectID string  `json:"object_id"`
	Regex    *string `json:"regex,omitempty"`
}

type BoundsReq struct {
	ObjectID string `json:"object_id"`
}

type ObjectDataReq struct {
	ObjectID string `json:"object_id"`
}

type RegionOfReq struct {
	ObjectID string `json:"object_id"`
}

type RegionFromPointReq struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

func (q Query) AllObjects(ctx context.Context, regex *string) (AllObjectsResp, error) {
	req := AllObjectsReq{
		Regex: regex,
	}
	resp, err := requestData[AllObjectsResp](q, ctx, QueryAllObjects, req)
	if err != nil {
		return AllObjectsResp{}, err
	}
	return resp, nil
}

func (q Query) AllObjectsOriented(ctx context.Context, regex *string) (AllObjectsOrientedResp, error) {
	req := AllObjectsOrientedReq{
		Regex: regex,
	}
	resp, err := requestData[AllObjectsOrientedResp](q, ctx, QueryAllObjectsOriented, req)
	if err != nil {
		return AllObjectsOrientedResp{}, err
	}
	return resp, nil
}

func (q Query) RegionOf(ctx context.Context, objectID string) (string, error) {
	req := RegionOfReq{
		ObjectID: objectID,
	}
	resp, err := requestData[RegionOfResp](q, ctx, QueryRegionOf, req)
	if err != nil {
		return "", err
	}
	return resp.Region, nil
}

func (q Query) RegionFromPoint(ctx context.Context, x, y, z float64) (string, error) {
	req := RegionFromPointReq{
		X: x,
		Y: y,
		Z: z,
	}
	resp, err := requestData[RegionOfResp](q, ctx, QueryRegionFromPoint, req)
	if err != nil {
		return "", err
	}
	return resp.Region, nil
}

func (q Query) NearbyObjects(ctx context.Context, region string, x, y, radiusMeters float64, regex *string) ([]Object, error) {
	req := NearbyReq{
		Region:       region,
		X:            x,
		Y:            y,
		RadiusMeters: radiusMeters,
		Regex:        regex,
	}
	return requestData[[]Object](q, ctx, QueryNearby, req)
}

func (q Query) NearbyObjectsOf(ctx context.Context, objectID string, radiusMeters float64, regex *string) ([]ObjectOriented, error) {
	req := NearbyOfReq{
		ObjectID:     objectID,
		RadiusMeters: radiusMeters,
		Regex:        regex,
	}
	return requestData[[]ObjectOriented](q, ctx, QueryNearbyOf, req)
}

func (q Query) ObjectsWithinArea(ctx context.Context, region string, areaID string, regex *string) ([]Object, error) {
	req := WithinAreaReq{
		Region: region,
		AreaID: areaID,
		Regex:  regex,
	}
	return requestData[[]Object](q, ctx, QueryWithinArea, req)
}

func (q Query) IntersectingObjects(ctx context.Context, objectID string, regex *string) ([]Object, error) {
	req := IntersectingReq{
		ObjectID: objectID,
		Regex:    regex,
	}
	return requestData[[]Object](q, ctx, QueryIntersecting, req)
}

func (q Query) ObjectBounds(ctx context.Context, objectID string) (*OrientedBounds, error) {
	req := BoundsReq{
		ObjectID: objectID,
	}
	resp, err := requestData[OrientedBounds](q, ctx, QueryObjectBounds, req)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (q Query) ObjectData(ctx context.Context, objectID string) (*Object, error) {
	req := ObjectDataReq{
		ObjectID: objectID,
	}
	resp, err := requestData[Object](q, ctx, QueryObjectData, req)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
