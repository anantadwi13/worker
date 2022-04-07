package workers

import "time"

func ptrDuration(duration time.Duration) *time.Duration {
	return &duration
}
