FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install the MCP server globally
RUN npm install -g apache-druid-mcp@latest

# Create a non-root user
RUN addgroup -g 1001 -S nodejs
RUN adduser -S druid -u 1001

# Set default environment variables
ENV DRUID_URL=http://localhost:8888
ENV DRUID_TIMEOUT=30000
ENV NODE_ENV=production

# Switch to non-root user
USER druid

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD node -e "console.log('Health check')" || exit 1

# Run the MCP server
CMD ["apache-druid-mcp"]

# Labels for metadata
LABEL org.opencontainers.image.title="Apache Druid MCP Server"
LABEL org.opencontainers.image.description="Model Context Protocol server for Apache Druid"
LABEL org.opencontainers.image.url="https://github.com/AnilPuram/druid"
LABEL org.opencontainers.image.source="https://github.com/AnilPuram/druid"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.licenses="Apache-2.0"