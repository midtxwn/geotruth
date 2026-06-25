package natsquery

import (
	"context"

	pubDomain "github.com/midtxwn/geotruth/pkg/domain"
)

type Area = pubDomain.Area

type AllAreasResp struct {
	Regions map[string][]pubDomain.Area `json:"regions"`
}

type AllAreasReq struct {
	Regex *string `json:"regex,omitempty"`
}

type AreasAtPointReq struct {
	Region string  `json:"region"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Regex  *string `json:"regex,omitempty"`
}

type AreasContainingReq struct {
	ObjectID string  `json:"object_id"`
	Regex    *string `json:"regex,omitempty"`
}

type NearbyAreasReq struct {
	Region       string  `json:"region"`
	X            float64 `json:"x"`
	Y            float64 `json:"y"`
	RadiusMeters float64 `json:"radius_meters"`
	Regex        *string `json:"regex,omitempty"`
}

type AreaReq struct {
	AreaID string `json:"area_id"`
}

func (q Query) AllAreas(ctx context.Context, regex *string) (AllAreasResp, error) {
	req := AllAreasReq{
		Regex: regex,
	}
	resp, err := requestData[AllAreasResp](q, ctx, QueryAllAreas, req)
	if err != nil {
		return AllAreasResp{}, err
	}
	return resp, nil
}

func (q Query) NearbyAreas(ctx context.Context, region string, x, y, radiusMeters float64, regex *string) ([]Area, error) {
	req := NearbyAreasReq{
		Region:       region,
		X:            x,
		Y:            y,
		RadiusMeters: radiusMeters,
		Regex:        regex,
	}
	return requestData[[]Area](q, ctx, QueryNearbyAreas, req)
}

func (q Query) AreasAtPoint(ctx context.Context, region string, x, y float64, regex *string) ([]Area, error) {
	req := AreasAtPointReq{
		Region: region,
		X:      x,
		Y:      y,
		Regex:  regex,
	}
	return requestData[[]Area](q, ctx, QueryAreasAtPoint, req)
}

func (q Query) AreasContainingObject(ctx context.Context, objectID string, regex *string) ([]Area, error) {
	req := AreasContainingReq{
		ObjectID: objectID,
		Regex:    regex,
	}
	return requestData[[]Area](q, ctx, QueryAreasContainingObj, req)
}

func (q Query) Area(ctx context.Context, areaID string) (*Area, error) {
	req := AreaReq{
		AreaID: areaID,
	}
	resp, err := requestData[Area](q, ctx, QueryArea, req)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
