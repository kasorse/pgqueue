package logger

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type contextKeyType string

// contextKey is the key to set and get LogPrefixer instance into context
const contextKey contextKeyType = "__logprefixer"

var (
	client = log.New()
)

func SetLevel(level log.Level) {
	client.SetLevel(level)
}

func SetJSONFormatter() {
	client.SetFormatter(&log.JSONFormatter{})
}

// WrapContext returns new context with meta information support
func WrapContext(ctx context.Context, prologue string) context.Context {
	lp := newLogPrefixer().setPrologue(prologue)
	return context.WithValue(ctx, contextKey, lp)
}

// SetItems appends or replaces attributes to meta information
func SetItems(ctx context.Context, items map[string]interface{}) {
	if lp, _ := ctx.Value(contextKey).(*logPrefixer); lp != nil {
		// TODO: don't regenerate lp.prefix in each iteration
		for key, value := range items {
			lp.setItem(key, value)
		}
	}
}

// getPrefix prepare prefix with meta information
func getPrefix(ctx context.Context) string {
	prefix := ""
	if lp, _ := ctx.Value(contextKey).(*logPrefixer); lp != nil {
		prefix = lp.getPrefix()
	}
	return prefix
}

// Infof implements logging with meta information support
func Infof(ctx context.Context, format string, args ...interface{}) {
	prefix := getPrefix(ctx)
	client.Infof(prefix+format, args...)
}

// Warnf implements logging with meta information support
func Warnf(ctx context.Context, format string, args ...interface{}) {
	prefix := getPrefix(ctx)
	client.Warnf(prefix+format, args...)
}

// Debugf implements logging with meta information support
func Debugf(ctx context.Context, format string, args ...interface{}) {
	prefix := getPrefix(ctx)
	client.Debugf(prefix+format, args...)
}

// Errorf implements logging with meta information support
func Errorf(ctx context.Context, format string, args ...interface{}) {
	prefix := getPrefix(ctx)
	client.Errorf(prefix+format, args...)
}

// Fatalf implements logging with meta information support
func Fatalf(ctx context.Context, format string, args ...interface{}) {
	prefix := getPrefix(ctx)
	client.Fatalf(prefix+format, args...)
}
