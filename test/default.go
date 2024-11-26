package test

import (
	"context"

	"github.com/antinvestor/matrix/setup/config"
)

func PrepareDefaultDSConnections(ctx context.Context) (config.DefaultOpts, func(), error) {

	cacheConnStr, closeCache, err := PrepareRedisDataSourceConnection(ctx)
	if err != nil {
		return config.DefaultOpts{}, nil, err
	}

	queueConnStr, closeQueue, err := PrepareNatsDataSourceConnection(ctx)
	if err != nil {
		defer closeCache()
		return config.DefaultOpts{}, nil, err
	}

	dbConnStr, closeDb, err := PrepareDatabaseDSConnection(ctx)
	if err != nil {
		defer closeCache()
		defer closeQueue()
		return config.DefaultOpts{}, nil, err
	}

	return config.DefaultOpts{
			DatabaseConnectionStr: dbConnStr,
			QueueConnectionStr:    queueConnStr,
			CacheConnectionStr:    cacheConnStr,
		}, func() {
			closeCache()
			closeQueue()
			closeDb()
		}, nil
}
