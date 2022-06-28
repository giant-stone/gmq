package gmq_test

import (
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/stretchr/testify/require"
)

func TestSimulatedClock(t *testing.T) {
	now := time.Now()

	samples := []struct {
		initTime  time.Time
		advanceBy time.Duration
		wantTime  time.Time
	}{
		{
			initTime:  now,
			advanceBy: 10 * time.Second,
			wantTime:  now.Add(10 * time.Second),
		},
		{
			initTime:  now,
			advanceBy: -5 * time.Second,
			wantTime:  now.Add(-5 * time.Second),
		},
	}

	for _, sample := range samples {
		c := gmq.NewSimulatedClock(sample.initTime)
		require.Equal(t, sample.initTime, c.Now(), "Now")

		c.AdvanceTime(sample.advanceBy)
		require.Equal(t, sample.wantTime, c.Now(), "AdvanceTime")
	}
}
