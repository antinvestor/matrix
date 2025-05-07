package sqlutil

import "gorm.io/gorm"

// defaultTransaction implements the Transaction interface for gorm.DB
type defaultTransaction struct {
	txn *gorm.DB
}

// Commit implements the Transaction Commit method for gorm
func (dt *defaultTransaction) Commit() error {
	return dt.txn.Commit().Error
}

// Rollback implements the Transaction Rollback method for gorm
func (dt *defaultTransaction) Rollback() error {
	return dt.txn.Rollback().Error
}

// NewTransaction creates a new Transaction from a gorm.DB transaction
func newDefaultTransaction(txn *gorm.DB) *defaultTransaction {
	return &defaultTransaction{txn: txn}
}
