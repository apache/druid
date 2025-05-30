# Apache Druid MCP Server

A Model Context Protocol (MCP) server that provides tools and resources for querying and managing Apache Druid datasources. This server enables AI assistants to interact with Apache Druid clusters through a standardized interface.

## Features

### 🔧 Tools
- **execute_sql_query**: Execute SQL queries against Druid datasources
- **execute_native_query**: Execute native JSON queries using Druid's query language
- **list_datasources**: Get a list of all available datasources
- **get_datasource_metadata**: Retrieve detailed metadata for specific datasources
- **get_segments**: Get segment information for datasources
- **test_connection**: Test connectivity to the Druid cluster

### 📊 Resources
- **Cluster Status**: Real-time cluster health and status information
- **Datasource Metadata**: Schema, column information, and statistics for each datasource
- **Segment Information**: Detailed segment-level data for monitoring and optimization

## Installation

### From source
```bash
# Clone the repository
git clone https://github.com/apache/druid.git
cd druid/mcp-server

# Install dependencies
npm install

# Build the server
npm run build

# Install globally (optional)
npm link
```

### For development
```bash
# Install dependencies
npm install

# Build and watch for changes
npm run dev

# Type check
npm run type-check

# Lint code
npm run lint
```

## Configuration

The server is configured through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DRUID_URL` | Druid broker URL | `http://localhost:8888` |
| `DRUID_USERNAME` | Optional username for authentication | - |
| `DRUID_PASSWORD` | Optional password for authentication | - |
| `DRUID_TIMEOUT` | Request timeout in milliseconds | `30000` |

## Usage

### Running the Server

```bash
# Using default configuration (localhost:8888)
apache-druid-mcp

# With custom Druid URL
DRUID_URL=http://your-druid-broker:8888 apache-druid-mcp

# With authentication
DRUID_URL=https://secure-druid.example.com \
DRUID_USERNAME=admin \
DRUID_PASSWORD=secret \
apache-druid-mcp
```

### Integration with AI Assistants

The server implements the Model Context Protocol, making it compatible with AI assistants that support MCP. Configure your AI assistant to connect to this server to enable Druid querying capabilities.

## API Examples

### SQL Queries
```sql
-- Get top 10 pages by page views
SELECT page, SUM(views) as total_views 
FROM wikipedia 
GROUP BY page 
ORDER BY total_views DESC 
LIMIT 10;

-- Time series analysis
SELECT 
  TIME_FLOOR(__time, 'PT1H') as hour,
  COUNT(*) as events,
  SUM(views) as total_views
FROM wikipedia 
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY TIME_FLOOR(__time, 'PT1H')
ORDER BY hour;
```

### Native Queries
```json
{
  "queryType": "topN",
  "dataSource": "wikipedia",
  "dimension": "page",
  "threshold": 10,
  "metric": "views",
  "granularity": "all",
  "aggregations": [
    {
      "type": "longSum",
      "name": "views",
      "fieldName": "views"
    }
  ],
  "intervals": ["2023-01-01/2024-01-01"]
}
```

## Development

### Building
```bash
npm run build
```

### Development Mode
```bash
npm run dev
```

### Type Checking
```bash
npm run type-check
```

### Linting
```bash
npm run lint
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   AI Assistant  │◄──►│  MCP Server      │◄──►│  Apache Druid   │
│                 │    │  (This Package)  │    │  Cluster        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

The server acts as a bridge between AI assistants and Apache Druid, translating MCP requests into appropriate Druid API calls.

## Supported Druid APIs

- **SQL API** (`/druid/v2/sql`): For SQL query execution
- **Native Query API** (`/druid/v2`): For native JSON queries
- **Datasources API** (`/druid/v2/datasources`): For listing and metadata retrieval
- **Segments API** (`/druid/v2/segments`): For segment information
- **Status API** (`/status`): For health checks

## Error Handling

The server provides comprehensive error handling:
- **Connection Errors**: Clear messages when unable to connect to Druid
- **Query Errors**: Detailed error information from Druid query failures
- **Authentication Errors**: Proper handling of authentication failures
- **Timeout Errors**: Configurable timeouts with clear error messages

## Security Considerations

- Supports HTTP Basic Authentication
- Environment variable-based configuration (never hardcode credentials)
- Request timeout limits to prevent resource exhaustion
- Input validation on all query parameters

## Contributing

This MCP server is part of the Apache Druid project. Please refer to the main Druid repository for contribution guidelines.

## License

Apache License 2.0 - see the LICENSE file for details.

## Related Links

- [Apache Druid](https://druid.apache.org/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Druid SQL Documentation](https://druid.apache.org/docs/latest/querying/sql.html)
- [Druid Native Queries](https://druid.apache.org/docs/latest/querying/querying.html)