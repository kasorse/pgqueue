package pgqueue

import (
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

// ErrInvalidCronSchedule is returned when a cron expression cannot be parsed or
// can never fire.
var ErrInvalidCronSchedule = errors.New("invalid cron schedule")

// ValidateCronSchedule checks that expr is a schedule this package can run.
//
// Accepted forms are the ones understood by cron.ParseStandard: the standard
// five-field spec ("0 0 * * *" — every day at 00:00), the "@daily"/"@hourly"
// descriptors, "@every 30m" intervals, and an optional "CRON_TZ=Europe/Moscow"
// prefix that pins the schedule to a time zone.
func ValidateCronSchedule(expr string) error {
	_, err := nextCronFire(expr, time.Now())
	return err
}

// nextCronFire returns the first moment expr fires strictly after the given time.
//
// The caller must pass a local time (time.Now(), never time.Now().UTC()): the
// queue stores "timestamp without time zone" columns, so the wall clock written
// to the database is the one carried by the time.Time handed to the driver, and
// a spec without a CRON_TZ prefix is evaluated in after.Location(). Passing a
// UTC time would read "0 3 * * *" as 03:00 UTC and store a consistently wrong
// wall clock.
func nextCronFire(expr string, after time.Time) (time.Time, error) {
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, errors.Wrapf(ErrInvalidCronSchedule, "cannot parse %q: %v", expr, err)
	}

	next := schedule.Next(after)
	// Next reports the zero time for a spec that parses but can never match,
	// such as "0 0 30 2 *". Storing it would leave the schedule permanently due
	// and spawn a task on every maintenance tick.
	if next.IsZero() {
		return time.Time{}, errors.Wrapf(ErrInvalidCronSchedule, "%q never fires", expr)
	}

	return next, nil
}
