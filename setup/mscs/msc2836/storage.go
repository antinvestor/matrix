package msc2836

import (
	"context"
	"fmt"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/postgres"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/shared"

	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup/config"
)

// NewDatabase loads the database for msc2836
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbOpts *config.DatabaseOptions) (shared.Database, error) {

	switch {
	case dbOpts.ConnectionString.IsPostgres():
		return postgres.NewDatabase(ctx, conMan, dbOpts)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
