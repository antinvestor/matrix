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
	cm ConnectionManager
}

// NewDefaultWriter returns a new dummy writer.
func NewDefaultWriter(cm ConnectionManager) Writer {
	return &DefaultWriter{
		cm: cm,
	}
}

func (w *DefaultWriter) Do(ctx context.Context, f func(ctx context.Context) error, opts ...*WriterOption) error {
	if w.cm != nil {

		ctx0, txn, err := w.cm.BeginTx(ctx, opts...)
		if err != nil {
			return err
		}

		return WithTransaction(ctx0, txn, func(ctx context.Context) error {
			return f(ctx)
		})
	} else {
		return f(ctx)
	}
}
