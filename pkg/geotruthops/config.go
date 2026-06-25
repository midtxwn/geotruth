package geotruthops

import "time"

type PressureMonitorConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	WarnRatio          float64       `mapstructure:"warnRatio"`
	CriticalRatio      float64       `mapstructure:"criticalRatio"`
	MinRefreshInterval time.Duration `mapstructure:"minRefreshInterval"`
	MinBytesDelta      uint64        `mapstructure:"minBytesDelta"`
}
