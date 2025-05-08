package sqlutil

import (
	"context"
)

// DefaultWriter implements sqlutil.Writer.
// The DefaultWriter is designed to allow reuse of the sqlutil.Writer
// interface but, unlike ExclusiveWriter, it will not guarantee
// writer exclusivity. This is fine in PostgreSQL where overlapping
// transactions and writes are acceptable.
type DefaultWriter struct {
}

// NewDefaultWriter returns a new dummy writer.
func NewDefaultWriter() Writer {
	return &DefaultWriter{}
}

func (w *DefaultWriter) Do(ctx context.Context, cm *Connections, f func(ctx context.Context) error) error {
	if cm != nil {
		return WithTransaction(ctx, cm, func(ctx context.Context) error {
			return f(ctx)
		})
	} else {
		return f(ctx)
	}
}
