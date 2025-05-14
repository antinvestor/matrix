// Copyright 2020 The Global.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlutil

import (
	"context"
	"errors"
	"github.com/pitabwire/frame"
)

type contextKey string

func (c contextKey) String() string {
	return "matrix/" + string(c)
}

const ctxKeyTransaction = contextKey("transactionKey")
const ctxKeyTransactionStack = contextKey("transactionStackKey")

// ErrUserExists is returned if a username already exists in the database.
var ErrUserExists = errors.New("username already exists")

// TransactionStack represents a stack of transactions to enable proper nesting
type TransactionStack struct {
	transactions []Transaction
}

// Push adds a new transaction to the top of the stack
func (s *TransactionStack) Push(txn Transaction) {
	s.transactions = append(s.transactions, txn)
}

// Pop removes and returns the transaction at the top of the stack
func (s *TransactionStack) Pop() Transaction {
	if len(s.transactions) == 0 {
		return nil
	}

	last := len(s.transactions) - 1
	txn := s.transactions[last]
	s.transactions = s.transactions[:last]
	return txn
}

// Peek returns the transaction at the top of the stack without removing it
func (s *TransactionStack) Peek() Transaction {
	if len(s.transactions) == 0 {
		return nil
	}

	return s.transactions[len(s.transactions)-1]
}

// IsEmpty returns true if the stack has no transactions
func (s *TransactionStack) IsEmpty() bool {
	return len(s.transactions) == 0
}

// GetTransactionStack retrieves the transaction stack from the context or creates a new one
func GetTransactionStack(ctx context.Context) *TransactionStack {
	if stack, ok := ctx.Value(ctxKeyTransactionStack).(*TransactionStack); ok {
		return stack
	}
	return &TransactionStack{}
}

// WithTransactionStack adds a transaction stack to the context
func WithTransactionStack(ctx context.Context, stack *TransactionStack) context.Context {
	return context.WithValue(ctx, ctxKeyTransactionStack, stack)
}

// A Transaction is something that can be committed or rolledback.
type Transaction interface {
	// Commit the transaction
	Commit() error
	// Rollback the transaction.
	Rollback() error
}

// EndTransaction ends a transaction.
// If the transaction succeeded then it is committed, otherwise it is rolledback.
// You MUST check the error returned from this function to be sure that the transaction
// was applied correctly. For example, 'database is locked' errors in sqlite will happen here.
func EndTransaction(txn Transaction, succeeded *bool) error {
	if *succeeded {
		return txn.Commit()
	} else {
		return txn.Rollback()
	}
}

// EndTransactionWithCheck ends a transaction and overwrites the error pointer if its value was nil.
// If the transaction succeeded then it is committed, otherwise it is rolledback.
// Designed to be used with defer (see EndTransaction otherwise).
func EndTransactionWithCheck(txn Transaction, succeeded *bool, err *error) {

	e := EndTransaction(txn, succeeded)
	if e != nil && *err == nil {
		*err = e
	}
}

// WithTransaction runs a block of code passing in an SQL transaction
// If the code returns an error or panics then the transactions is rolledback
// Otherwise the transaction is committed.
func WithTransaction(ctx context.Context, txn Transaction, fn func(ctx context.Context) error) (err error) {

	stack := GetTransactionStack(ctx)
	stack.Push(txn)
	defer func() {
		stack.Pop()
	}()

	succeeded := false
	defer EndTransactionWithCheck(txn, &succeeded, &err)

	err = fn(ctx)
	if err != nil {
		return
	}

	succeeded = true
	return
}

func ErrorIsNoRows(err error) bool {
	return frame.ErrorIsNoRows(err)
}
