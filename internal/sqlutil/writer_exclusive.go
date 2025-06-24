package sqlutil

import (
	"context"
	"errors"
	"sync/atomic"
)

// ExclusiveWriter implements sqlutil.Writer.
// ExclusiveWriter allows queuing database writes so that you don't
// contend on database locks in, e.g. SQLite. Only one task will run
// at a time on a given ExclusiveWriter.
type ExclusiveWriter struct {
	cm      ConnectionManager
	running atomic.Bool
	todo    chan transactionWriterTask
}

func NewExclusiveWriter(cm ConnectionManager) Writer {
	return &ExclusiveWriter{
		cm:   cm,
		todo: make(chan transactionWriterTask),
	}
}

// transactionWriterTask represents a specific task.
type transactionWriterTask struct {
	ctx  context.Context
	opts []*WriterOption
	f    func(ctx context.Context) error
	wait chan error
}

// Do queues a task to be run by a TransactionWriter. The function
// provided will be ran within a transaction as supplied by the
// txn parameter if one is supplied, and if not, will take out a
// new transaction from the database supplied in the database
// parameter. Either way, this will block until the task is done.
func (w *ExclusiveWriter) Do(ctx context.Context, f func(ctx context.Context) error, opts ...*WriterOption) error {
	if w.todo == nil {
		return errors.New("not initialised")
	}
	if !w.running.Load() {
		go w.run()
	}
	task := transactionWriterTask{
		ctx:  ctx,
		opts: opts,
		f:    f,
		wait: make(chan error, 1),
	}
	w.todo <- task
	return <-task.wait
}

// run processes the tasks for a given transaction writer. Only one
// of these goroutines will run at a time. A transaction will be
// opened using the database object from the task and then this will
// be passed as a parameter to the task function.
func (w *ExclusiveWriter) run() {
	if !w.running.CompareAndSwap(false, true) {
		return
	}

	defer w.running.Store(false)
	for task := range w.todo {
		if w.cm != nil {

			task.wait <- func(ctx context.Context) error {
				ctx0, txn, err := w.cm.BeginTx(ctx, task.opts...)
				if err != nil {
					return err
				}
				return WithTransaction(ctx0, txn, func(ctx context.Context) error {
					return task.f(ctx)
				})
			}(task.ctx)
		} else {
			task.wait <- task.f(task.ctx)
		}
		close(task.wait)
	}
}
