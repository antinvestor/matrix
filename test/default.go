package test

import (
	"context"
	"math/rand"
	"time"

	"github.com/antinvestor/matrix/setup/config"
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(result)
}

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
			QueuePrefix:           randomString(12),
			CacheConnectionStr:    cacheConnStr,
		}, func() {
			closeCache()
			closeQueue()
			closeDb()
		}, nil
}
