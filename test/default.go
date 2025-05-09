package test

import (
	"context"

	"github.com/antinvestor/matrix/setup/config"
)

func PrepareDefaultDSConnections(ctx context.Context, testOpts DependancyOption) (config.DefaultOpts, func(ctx context.Context), error) {

	cacheConnStr, closeCache, err := PrepareCacheConnection(ctx, testOpts)
	if err != nil {
		return config.DefaultOpts{}, nil, err
	}

	queueConnStr, closeQueue, err := PrepareQueueConnection(ctx, testOpts)
	if err != nil {
		defer closeCache(ctx)
		return config.DefaultOpts{}, nil, err
	}

	dbConnStr, closeDb, err := PrepareDatabaseConnection(ctx, testOpts)
	if err != nil {
		defer closeCache(ctx)
		defer closeQueue(ctx)
		return config.DefaultOpts{}, nil, err
	}

	return config.DefaultOpts{
			DatabaseConnectionStr: dbConnStr,
			QueueConnectionStr:    queueConnStr,
			CacheConnectionStr:    cacheConnStr,
		}, func(ctx context.Context) {
			closeCache(ctx)
			closeQueue(ctx)
			closeDb(ctx)
		}, nil
}
