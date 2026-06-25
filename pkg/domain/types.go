package domain

type Triangle [3]Point

type Object struct {
	ID     string  `json:"id"`
	Region *string `json:"region"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Z      float64 `json:"z"`
	RotY   float64 `json:"rot_y"`
}

type ObjectDimensions struct {
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type Area struct {
	ID        string     `json:"id"`
	Region    string     `json:"region"`
	Triangles []Triangle `json:"triangles,omitempty"`
	CX        float64    `json:"cx,omitempty"`
	CY        float64    `json:"cy,omitempty"`
	PX        float64    `json:"px,omitempty"`
	PY        float64    `json:"py,omitempty"`
}
