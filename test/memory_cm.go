package test

import (
	"context"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/frame"
	"gorm.io/gorm"
)

type MemoryConnectionManager struct {
	ds config.DataSource
}

func (m MemoryConnectionManager) Do(ctx context.Context, f func(ctx context.Context) error, _ ...*sqlutil.WriterOption) error {
	return f(ctx)
}

func (m MemoryConnectionManager) DS() *config.DataSource {
	return &m.ds
}

func (m MemoryConnectionManager) Connection(_ context.Context, _ bool) *gorm.DB {
	return nil
}

func (m MemoryConnectionManager) BeginTx(ctx context.Context, _ ...*sqlutil.WriterOption) (context.Context, sqlutil.Transaction, error) {
	return ctx, nil, nil
}

func (m MemoryConnectionManager) FromOptions(_ context.Context, _ *config.DatabaseOptions) (sqlutil.ConnectionManager, error) {
	return m, nil
}

func (m MemoryConnectionManager) Collect(_ ...*frame.MigrationPatch) error {
	return nil
}

func (m MemoryConnectionManager) Migrate(_ context.Context) error {
	return nil
}

func NewInMemoryConnectionManager() sqlutil.ConnectionManager {
	return &MemoryConnectionManager{
		ds: config.DataSource("memory"),
	}
}
