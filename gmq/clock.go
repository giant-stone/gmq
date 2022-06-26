package gmq

import "time"

type Clock interface {
	Now() time.Time
}

func NewWallClock() Clock {
	return &WallClock{}
}

type WallClock struct{}

func (it *WallClock) Now() time.Time {
	return time.Now()
}
