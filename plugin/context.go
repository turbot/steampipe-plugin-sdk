package plugin

import (
	"context"
	"github.com/hashicorp/go-hclog"
)

//https://medium.com/@matryer/context-keys-in-go-5312346a868d
type contextKey string

func (c contextKey) String() string {
	return "plugin context key " + string(c)
}

var (
	ContextKeyLogger = contextKey("logger")
)

func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(ContextKeyLogger).(hclog.Logger)
}
