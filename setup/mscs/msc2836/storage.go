package msc2836

import (
	"context"
	"fmt"

	"github.com/antinvestor/matrix/setup/mscs/msc2836/postgres"
	"github.com/antinvestor/matrix/setup/mscs/msc2836/shared"

	"github.com/antinvestor/matrix/internal/sqlutil"
)

// NewDatabase loads the database for msc2836
func NewDatabase(ctx context.Context, cm *sqlutil.Connections) (shared.Database, error) {

	switch {
	case cm.DS().IsPostgres():
		return postgres.NewDatabase(ctx, cm)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
