package postgres

import (
	"context"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/shared"
)

// Migrations Centralized migrations for msc2836 tables
var Migrations = []sqlutil.Migration{
	{
		Version:   "msc2836_001_nodes",
		QueryUp:   msc2836NodesSchema,
		QueryDown: msc2836NodesSchemaRevert,
	},
	{
		Version:   "msc2836_002_edges",
		QueryUp:   msc2836EdgesSchema,
		QueryDown: msc2836EdgesSchemaRevert,
	},
}

// NewDatabase opens a postgres database.
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (shared.Database, error) {
	db, writer, err := conMan.Connection(ctx, dbProperties)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db, Migrations...)
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}

	return NewPostgresMSC2836Repository(ctx, db, writer)
}
