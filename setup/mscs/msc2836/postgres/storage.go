package postgres

import (
	"context"
	"github.com/pitabwire/frame"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/shared"
)

// Migrations Centralised migrations for msc2836 tables
var Migrations = []frame.MigrationPatch{
	{
		Name:        "msc2836_001_nodes",
		Patch:       msc2836NodesSchema,
		RevertPatch: msc2836NodesSchemaRevert,
	},
	{
		Name:        "msc2836_002_edges",
		Patch:       msc2836EdgesSchema,
		RevertPatch: msc2836EdgesSchemaRevert,
	},
}

// NewDatabase opens a postgres database.
func NewDatabase(ctx context.Context, cm *sqlutil.Connections) (shared.Database, error) {

	err := cm.MigrateStrings(ctx, Migrations...)
	if err != nil {
		return nil, err
	}

	return NewPostgresMSC2836EdgeTable(cm), nil
}
