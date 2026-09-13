package pgqueue

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const everyDayAtMidnight = "0 0 * * *"

func TestNextCronFire(t *testing.T) {
	a := assert.New(t)

	// Wednesday
	base := time.Date(2026, time.September, 9, 14, 25, 30, 0, time.UTC)

	testCases := []struct {
		name     string
		expr     string
		expected time.Time
	}{
		{
			name:     "every day at midnight",
			expr:     everyDayAtMidnight,
			expected: time.Date(2026, time.September, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "every quarter of an hour",
			expr:     "*/15 * * * *",
			expected: time.Date(2026, time.September, 9, 14, 30, 0, 0, time.UTC),
		},
		{
			name:     "midnight of a weekday skips the weekend",
			expr:     "0 0 * * 1-5",
			expected: time.Date(2026, time.September, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "descriptor",
			expr:     "@daily",
			expected: time.Date(2026, time.September, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "interval",
			expr:     "@every 90s",
			expected: base.Add(90 * time.Second),
		},
		{
			name: "time zone prefix",
			expr: "CRON_TZ=Europe/Moscow " + everyDayAtMidnight,
			// midnight in Moscow (UTC+3) is 21:00 UTC of the previous day
			expected: time.Date(2026, time.September, 9, 21, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(_ *testing.T) {
			actual, err := nextCronFire(tc.expr, base)
			a.NoError(err)
			a.True(tc.expected.Equal(actual), "expected %v, actual %v", tc.expected, actual)
			a.True(actual.After(base), "next fire must be strictly after the given time")
		})
	}
}

func TestValidateCronSchedule(t *testing.T) {
	a := assert.New(t)

	testCases := []struct {
		name  string
		expr  string
		valid bool
	}{
		{name: "five fields", expr: everyDayAtMidnight, valid: true},
		{name: "descriptor", expr: "@hourly", valid: true},
		{name: "interval", expr: "@every 30m", valid: true},
		{name: "empty", expr: "", valid: false},
		{name: "garbage", expr: "not a cron expression", valid: false},
		{name: "too few fields", expr: "0 0 *", valid: false},
		{name: "minute out of range", expr: "99 0 * * *", valid: false},
		// parses, but February never has a thirtieth day
		{name: "never fires", expr: "0 0 30 2 *", valid: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(_ *testing.T) {
			err := ValidateCronSchedule(tc.expr)
			if tc.valid {
				a.NoError(err)
				return
			}
			a.Error(err)
			a.Equal(ErrInvalidCronSchedule, errors.Cause(err))
		})
	}
}
