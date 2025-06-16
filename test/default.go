package test

import (
	"context"
	"strings"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/pitabwire/util"
)

func PrepareDefaultDSConnections(ctx context.Context, testOpts DependancyOption) (config.DefaultOpts, func(ctx context.Context), error) {

	randomnessPrefix := strings.ToLower(util.RandomString(7))
	configDefaults := config.DefaultOpts{
		RandomnessPrefix: randomnessPrefix,
	}

	cacheConn, closeCache, err := PrepareCacheConnection(ctx, randomnessPrefix, testOpts)
	if err != nil {
		return configDefaults, nil, err
	}

	configDefaults.DSCacheConn = cacheConn

	queueConn, closeQueue, err := PrepareQueueConnection(ctx, randomnessPrefix, testOpts)
	if err != nil {
		defer closeCache(ctx)
		return configDefaults, nil, err
	}

	configDefaults.DSQueueConn = queueConn

	dbConn, closeDb, err := PrepareDatabaseConnection(ctx, randomnessPrefix, testOpts)
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
