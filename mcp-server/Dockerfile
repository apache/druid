# Use Node.js 18 Alpine for smaller image size
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files first for better caching
COPY package*.json ./

# Install all dependencies (including dev dependencies for build)
RUN npm ci

# Copy source code
COPY . .

# Build TypeScript to JavaScript
RUN npm run build

# Remove dev dependencies after build
RUN npm prune --production

# Expose port (default 3000, can be overridden)
EXPOSE 3000

# Run the MCP server
ENTRYPOINT ["node", "dist/index.js"]

# Labels for metadata
LABEL org.opencontainers.image.title="Apache Druid MCP Server"
LABEL org.opencontainers.image.description="Model Context Protocol server for Apache Druid"
LABEL org.opencontainers.image.url="https://github.com/apache/druid"
LABEL org.opencontainers.image.source="https://github.com/apache/druid"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.licenses="Apache-2.0"