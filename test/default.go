package test

import (
	"context"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

func PrepareDefaultDSConnections(ctx context.Context, testOpts DependancyOption) (config.DefaultOpts, func(ctx context.Context), error) {

	configDefaults := config.DefaultOpts{
		QueuePrefix: util.RandomString(7),
	}

	cacheConn, closeCache, err := PrepareCacheConnection(ctx, testOpts)
	if err != nil {
		return configDefaults, nil, err
	}

	configDefaults.DSCacheConn = cacheConn

	queueConn, closeQueue, err := PrepareQueueConnection(ctx, testOpts)
	if err != nil {
		defer closeCache(ctx)
		return configDefaults, nil, err
	}

	configDefaults.DSQueueConn = queueConn

	dbConn, closeDb, err := PrepareDatabaseConnection(ctx, testOpts)
	if err != nil {
		defer closeCache(ctx)
		defer closeQueue(ctx)
		return configDefaults, nil, err
	}

	configDefaults.DSDatabaseConn = dbConn

	return configDefaults, func(ctx context.Context) {
		closeCache(ctx)
		closeQueue(ctx)
		closeDb(ctx)
	}, nil
}
