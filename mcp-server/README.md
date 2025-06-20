# Apache Druid MCP Server

Model Context Protocol (MCP) server for Apache Druid - provides tools and resources for querying and managing Druid datasources.

## Quick Start

### NPX (Recommended)

```bash
npx apache-druid-mcp
```

### Docker

```bash
# Build and run with Docker Compose
docker-compose up --build

# Or build and run manually
docker build -t apache-druid-mcp .
docker run -p 3000:3000 \
  -e DRUID_URL=http://localhost:8888 \
  apache-druid-mcp
```

### Local Development

```bash
npm install
npm run build
npm start
```

## Configuration

Environment variables:

- `DRUID_URL` - Druid router/broker URL (default: http://localhost:8888)
- `DRUID_USERNAME` - Optional authentication username
- `DRUID_PASSWORD` - Optional authentication password
- `DRUID_TIMEOUT` - Request timeout in ms (default: 30000)

## Usage

The MCP server provides tools for:

- **query_sql** - Execute SQL queries against Druid
- **list_datasources** - List all available datasources
- **describe_datasource** - Get schema information for a datasource
- **get_datasource_segments** - Get segment information for a datasource

## Examples

```bash
# Connect with Claude Desktop
npx apache-druid-mcp

# Connect to custom Druid instance
DRUID_URL=http://your-druid:8888 npx apache-druid-mcp
```

## License

Apache License 2.0 