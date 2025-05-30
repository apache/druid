# Apache Druid MCP Server

[![npm version](https://badge.fury.io/js/apache-druid-mcp.svg)](https://www.npmjs.com/package/apache-druid-mcp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Node.js Version](https://img.shields.io/badge/node-%3E%3D18.0.0-brightgreen)](https://nodejs.org/)

A Model Context Protocol (MCP) server that provides tools and resources for querying and managing Apache Druid datasources. This server enables AI assistants to interact with Apache Druid clusters through a standardized interface.

## 📚 Documentation

| Guide | Description |
|-------|-------------|
| [🚀 Quick Start](./QUICK_START.md) | Get connected in minutes! |
| [🔧 Integration Guide](./INTEGRATION_GUIDE.md) | Detailed setup for all clients |
| [🐳 Docker Setup](./INTEGRATION_GUIDE.md#-docker-deployment) | Containerized deployment |
| [☸️ Kubernetes](./INTEGRATION_GUIDE.md#️-kubernetes-deployment) | K8s deployment guides |

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Client Integration](#client-integration)
- [Configuration](#configuration)
- [API Examples](#api-examples)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

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

### From npm (Recommended)
```bash
npm install -g apache-druid-mcp
```

### From source
```bash
# Clone the repository
git clone https://github.com/AnilPuram/druid.git
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

## Client Integration

The server implements the Model Context Protocol, making it compatible with various AI assistants and development tools. Here's how to connect it to different clients:

### 🖥️ Claude Desktop

1. **Install the MCP server globally:**
   ```bash
   npm install -g apache-druid-mcp
   ```

2. **Configure Claude Desktop** by editing your MCP settings file:

   **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
   **Windows:** `%APPDATA%/Claude/claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "apache-druid": {
         "command": "apache-druid-mcp",
         "env": {
           "DRUID_URL": "http://localhost:8888",
           "DRUID_USERNAME": "your-username",
           "DRUID_PASSWORD": "your-password"
         }
       }
     }
   }
   ```

3. **Restart Claude Desktop** to load the new configuration.

### 🔧 VS Code with Continue

1. **Install the MCP server:**
   ```bash
   npm install -g apache-druid-mcp
   ```

2. **Install the Continue extension** in VS Code.

3. **Configure Continue** by editing `~/.continue/config.json`:
   ```json
   {
     "models": [...],
     "mcpServers": [
       {
         "name": "apache-druid",
         "command": "apache-druid-mcp",
         "env": {
           "DRUID_URL": "http://localhost:8888"
         }
       }
     ]
   }
   ```

4. **Restart VS Code** to apply the configuration.

### 🌐 Any MCP-Compatible Client

For other MCP-compatible clients, use these connection details:

**Command:** `apache-druid-mcp`
**Transport:** stdio
**Environment Variables:**
- `DRUID_URL`: Your Druid broker URL
- `DRUID_USERNAME`: Optional authentication username
- `DRUID_PASSWORD`: Optional authentication password
- `DRUID_TIMEOUT`: Request timeout in milliseconds (default: 30000)

### 🐳 Docker Integration

Run the MCP server in Docker:

```bash
# Build the image
docker build -t apache-druid-mcp .

# Run with environment variables
docker run -e DRUID_URL=http://your-druid:8888 apache-druid-mcp
```

### 🔧 Development Integration

For development environments, you can run the server directly:

```bash
# Set environment variables
export DRUID_URL=http://localhost:8888
export DRUID_USERNAME=admin
export DRUID_PASSWORD=secret

# Run the server
apache-druid-mcp
```

### ⚙️ Configuration Examples

**Local Development:**
```json
{
  "env": {
    "DRUID_URL": "http://localhost:8888"
  }
}
```

**Production with Authentication:**
```json
{
  "env": {
    "DRUID_URL": "https://druid.company.com:8888",
    "DRUID_USERNAME": "mcp-user",
    "DRUID_PASSWORD": "secure-password",
    "DRUID_TIMEOUT": "60000"
  }
}
```

**Docker/Kubernetes Environment:**
```json
{
  "env": {
    "DRUID_URL": "http://druid-broker.druid-namespace.svc.cluster.local:8888"
  }
}
```

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

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### 🐛 Reporting Issues
- Check [existing issues](https://github.com/AnilPuram/druid/issues) first
- Use the issue template when creating new issues
- Include reproduction steps and environment details

### 💻 Development Setup
```bash
# Fork and clone the repository
git clone https://github.com/your-username/druid.git
cd druid/mcp-server

# Install dependencies
npm install

# Run in development mode
npm run dev

# Run tests and linting
npm run type-check
npm run lint
```

### 🔄 Pull Requests
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Ensure all checks pass: `npm run build && npm run type-check && npm run lint`
5. Commit with clear messages
6. Push and create a Pull Request

### 📝 Documentation
- Update README.md for new features
- Add examples to INTEGRATION_GUIDE.md
- Update QUICK_START.md for setup changes

## 📄 License

Apache License 2.0 - see the [LICENSE](./LICENSE) file for details.

## 🔗 Related Links

- [📦 npm Package](https://www.npmjs.com/package/apache-druid-mcp)
- [🐙 GitHub Repository](https://github.com/AnilPuram/druid)
- [🔧 Apache Druid](https://druid.apache.org/)
- [🤖 Model Context Protocol](https://modelcontextprotocol.io/)
- [📖 Druid SQL Documentation](https://druid.apache.org/docs/latest/querying/sql.html)
- [🛠️ Druid Native Queries](https://druid.apache.org/docs/latest/querying/querying.html)

## ⭐ Support

If this project helps you, please consider:
- ⭐ Starring the repository
- 🐛 Reporting issues you encounter
- 💡 Suggesting new features
- 📖 Improving documentation
- 🤝 Contributing code

---

**Made with ❤️ for the Apache Druid and AI community**