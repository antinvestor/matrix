FROM nats:latest
# Set environment variables for NATS configuration
ENV NATS_USER=matrix \
    NATS_PASS=s3cr3t \
    NATS_PORT=4222

# Expose the default NATS port
EXPOSE 4222

# Default command to start NATS with the specified options
CMD ["--js", "--user", "${NATS_USER}", "--pass", "${NATS_PASS}"]
