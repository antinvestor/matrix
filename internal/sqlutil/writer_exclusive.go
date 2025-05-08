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
	running atomic.Bool
	todo    chan transactionWriterTask
}

func NewExclusiveWriter() Writer {
	return &ExclusiveWriter{
		todo: make(chan transactionWriterTask),
	}
}

// transactionWriterTask represents a specific task.
type transactionWriterTask struct {
	ctx  context.Context
	cm   *Connections
	f    func(ctx context.Context) error
	wait chan error
}

// Do queues a task to be run by a TransactionWriter. The function
// provided will be ran within a transaction as supplied by the
// txn parameter if one is supplied, and if not, will take out a
// new transaction from the database supplied in the database
// parameter. Either way, this will block until the task is done.
func (w *ExclusiveWriter) Do(ctx context.Context, cm *Connections, f func(ctx context.Context) error) error {
	if w.todo == nil {
		return errors.New("not initialised")
	}
	if !w.running.Load() {
		go w.run()
	}
	task := transactionWriterTask{
		cm:   cm,
		ctx:  ctx,
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
		if task.cm != nil {
			task.wait <- WithTransaction(task.ctx, task.cm, func(ctx context.Context) error {
				return task.f(ctx)
			})
		} else {
			task.wait <- task.f(task.ctx)
		}
		close(task.wait)
	}
}
