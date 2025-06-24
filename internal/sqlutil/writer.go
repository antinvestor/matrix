package sqlutil

import (
	"context"
	"database/sql"
)

// The Writer interface is designed to solve the problem of how
// to handle database writes for database engines that don't allow
// concurrent writes, e.g. SQLite.
//
// The interface has a single Do function which takes an optional
// database parameter, an optional transaction parameter and a
// required function parameter. The Writer will call the function
// provided when it is safe to do so, optionally providing a
// transaction to use.
//
// The Writer will call f() when it is safe to do so. The supplied
// "txn" will ALWAYS be passed through to f() via the context.
// Use the connection manager to obtain the connection. If one exists
// in the context then it will be re used.
//
// You MUST take particular care not to call Do() from within f()
// on the same Writer, or it will likely result in a deadlock.
type Writer interface {
	// Do queues up one or more database write operations within the
	// provided function to be executed when it is safe to do so.
	Do(ctx context.Context, f func(ctx context.Context) error, opts ...*WriterOption) error
}

// WriterOption is an optional parameter to Writer.Do
type WriterOption struct {
	SqlOpts []*sql.TxOptions
}
