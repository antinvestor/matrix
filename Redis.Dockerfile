FROM redis:latest
# Set default arguments
ENV REDIS_ARGS="--databases 10000 --requirepass s3cr3t --user matrix on >s3cr3t ~* +@all"

ENTRYPOINT ["sh", "-c", "redis-server $REDIS_ARGS"]
