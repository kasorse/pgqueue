package logger

import (
	"fmt"
	"strings"
	"sync"
)

// logPrefixer provides help for printing logs
type logPrefixer struct {
	prologue string
	prefix   string
	mu       sync.Mutex
	items    map[string]interface{}
}

// newLogPrefixer makes new instance
func newLogPrefixer() *logPrefixer {
	return &logPrefixer{
		prologue: "",
		prefix:   "",
		items:    make(map[string]interface{}),
	}
}

// getPrefix returns prefix
func (lp *logPrefixer) getPrefix() string {
	return lp.prologue + lp.prefix
}

// setBypassPrologue sets bypass prologue for usage in GetBypassPrefix
func (lp *logPrefixer) setPrologue(prologue string) *logPrefixer {
	if prologue != "" {
		lp.prologue = prologue + " "
	} else {
		lp.prologue = ""
	}

	return lp
}

// setItem appends or replaces attribute to prefix
func (lp *logPrefixer) setItem(key string, value interface{}) *logPrefixer {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	lp.items[key] = value
	lp.prefix = lp.generatePrefix()

	return lp
}

// generatePrefix make prefix by items
func (lp *logPrefixer) generatePrefix() string {
	if len(lp.items) == 0 {
		return ""
	}

	items := make([]string, 0, len(lp.items))
	for key, value := range lp.items {
		items = append(items, fmt.Sprintf("%s=%v", key, value))
	}

	return "[" + strings.Join(items, " ") + "] "
}
