# Apache Druid MCP Server - Integration Guide

This guide provides detailed instructions for integrating the Apache Druid MCP Server with various AI assistants and development tools.

## 📋 Prerequisites

1. **Apache Druid MCP Server installed:**
   ```bash
   npm install -g apache-druid-mcp
   ```

2. **Running Apache Druid cluster** with accessible broker endpoint

3. **Network connectivity** between the client and Druid cluster

## 🖥️ Claude Desktop Integration

### Step 1: Install Claude Desktop
Download Claude Desktop from [claude.ai](https://claude.ai/download)

### Step 2: Configure MCP Server

**Location of config file:**
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%/Claude/claude_desktop_config.json`
- **Linux:** `~/.config/Claude/claude_desktop_config.json`

**Basic Configuration:**
```json
{
  "mcpServers": {
    "apache-druid": {
      "command": "apache-druid-mcp",
      "env": {
        "DRUID_URL": "http://localhost:8888"
      }
    }
  }
}
```

**Production Configuration with Authentication:**
```json
{
  "mcpServers": {
    "apache-druid": {
      "command": "apache-druid-mcp",
      "env": {
        "DRUID_URL": "https://druid.company.com:8888",
        "DRUID_USERNAME": "claude-user",
        "DRUID_PASSWORD": "secure-password",
        "DRUID_TIMEOUT": "30000"
      }
    }
  }
}
```

### Step 3: Restart Claude Desktop

After saving the configuration, restart Claude Desktop. You should see the Druid tools available in the conversation.

### Step 4: Test the Integration

Try asking Claude:
- "List all available datasources in Druid"
- "Show me the schema for the wikipedia datasource"
- "Run a query to get the top 10 pages by views"

## 🔧 VS Code with Continue Extension

### Step 1: Install Continue Extension
1. Open VS Code
2. Go to Extensions (Ctrl+Shift+X)
3. Search for "Continue"
4. Install the Continue extension

### Step 2: Configure Continue

**Create/edit** `~/.continue/config.json`:

```json
{
  "models": [
    {
      "title": "Claude 3.5 Sonnet",
      "provider": "anthropic",
      "model": "claude-3-5-sonnet-20241022",
      "apiKey": "your-anthropic-api-key"
    }
  ],
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

### Step 3: Restart VS Code

Reload the window or restart VS Code to apply the configuration.

### Step 4: Use the Integration

1. Open the Continue panel (Ctrl+Shift+L)
2. Ask questions about your Druid data
3. The AI can now query Druid directly through the MCP server

## 🌐 Other MCP-Compatible Clients

### Cursor IDE

**Config location:** `~/.cursor/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "apache-druid": {
      "command": "apache-druid-mcp",
      "env": {
        "DRUID_URL": "http://localhost:8888"
      }
    }
  }
}
```

### Zed Editor

**Config location:** `~/.config/zed/settings.json`

```json
{
  "mcp": {
    "servers": {
      "apache-druid": {
        "command": "apache-druid-mcp",
        "env": {
          "DRUID_URL": "http://localhost:8888"
        }
      }
    }
  }
}
```

### Custom MCP Client

For building your own MCP client:

```typescript
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

const transport = new StdioClientTransport({
  command: 'apache-druid-mcp',
  env: {
    DRUID_URL: 'http://localhost:8888'
  }
});

const client = new Client({
  name: "my-druid-client",
  version: "1.0.0"
});

await client.connect(transport);

// Use the client
const tools = await client.listTools();
const result = await client.callTool({
  name: "execute_sql_query",
  arguments: {
    query: "SELECT * FROM wikipedia LIMIT 10"
  }
});
```

## 🐳 Docker Deployment

### Create Dockerfile

```dockerfile
FROM node:18-alpine

# Install the MCP server globally
RUN npm install -g apache-druid-mcp

# Set default environment variables
ENV DRUID_URL=http://localhost:8888
ENV DRUID_TIMEOUT=30000

# Expose any necessary ports (if running as HTTP server)
# EXPOSE 3000

# Run the MCP server
CMD ["apache-druid-mcp"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  druid-mcp:
    build: .
    environment:
      - DRUID_URL=http://druid-broker:8888
      - DRUID_USERNAME=mcp-user
      - DRUID_PASSWORD=secret
    depends_on:
      - druid-broker
    
  druid-broker:
    image: apache/druid:latest
    # ... Druid configuration
```

### Run with Docker

```bash
# Build and run
docker build -t apache-druid-mcp .
docker run -e DRUID_URL=http://your-druid:8888 apache-druid-mcp

# Or use docker-compose
docker-compose up
```

## ☸️ Kubernetes Deployment

### ConfigMap for Environment Variables

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: druid-mcp-config
data:
  DRUID_URL: "http://druid-broker.druid-namespace.svc.cluster.local:8888"
  DRUID_TIMEOUT: "30000"
```

### Secret for Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: druid-mcp-secret
type: Opaque
stringData:
  DRUID_USERNAME: "mcp-user"
  DRUID_PASSWORD: "secure-password"
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: druid-mcp-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app: druid-mcp-server
  template:
    metadata:
      labels:
        app: druid-mcp-server
    spec:
      containers:
      - name: druid-mcp
        image: apache-druid-mcp:latest
        envFrom:
        - configMapRef:
            name: druid-mcp-config
        - secretRef:
            name: druid-mcp-secret
```

## 🔒 Security Best Practices

### 1. Environment Variables
Store sensitive information in environment variables, not configuration files:

```bash
export DRUID_USERNAME="secure-user"
export DRUID_PASSWORD="complex-password"
apache-druid-mcp
```

### 2. Network Security
- Use HTTPS for production Druid endpoints
- Implement proper firewall rules
- Consider VPN or private networks for sensitive data

### 3. Authentication
- Create dedicated Druid users for MCP access
- Use least-privilege principles
- Rotate credentials regularly

### 4. Client Configuration
For Claude Desktop, use environment variables instead of hardcoding credentials:

```json
{
  "mcpServers": {
    "apache-druid": {
      "command": "apache-druid-mcp",
      "env": {
        "DRUID_URL": "${DRUID_URL}",
        "DRUID_USERNAME": "${DRUID_USERNAME}",
        "DRUID_PASSWORD": "${DRUID_PASSWORD}"
      }
    }
  }
}
```

## 🐛 Troubleshooting

### Common Issues

**1. "Command not found: apache-druid-mcp"**
```bash
# Make sure it's installed globally
npm install -g apache-druid-mcp

# Check if it's in PATH
which apache-druid-mcp
```

**2. "Cannot connect to Druid cluster"**
```bash
# Test connectivity
curl http://your-druid:8888/status

# Check environment variables
echo $DRUID_URL

# Test the MCP server directly
DRUID_URL=http://localhost:8888 apache-druid-mcp
```

**3. "Authentication failed"**
- Verify username/password are correct
- Check if the Druid cluster requires authentication
- Ensure the user has necessary permissions

**4. "MCP server not responding"**
- Check if the server process is running
- Verify the command path in client configuration
- Look at client logs for error messages

### Debug Mode

Run the MCP server with debug output:

```bash
DEBUG=* apache-druid-mcp
```

### Log Analysis

Client-side logs:
- **Claude Desktop:** Check application logs in the system
- **VS Code Continue:** Check Output panel for Continue extension
- **Custom clients:** Implement proper error handling and logging

## 📚 Additional Resources

- [Model Context Protocol Documentation](https://modelcontextprotocol.io/)
- [Apache Druid Documentation](https://druid.apache.org/docs/latest/)
- [Claude Desktop MCP Guide](https://claude.ai/docs/mcp)
- [Continue Extension Documentation](https://continue.dev/)

## 🤝 Support

For issues and questions:
- **GitHub Issues:** [https://github.com/AnilPuram/druid/issues](https://github.com/AnilPuram/druid/issues)
- **npm Package:** [https://www.npmjs.com/package/apache-druid-mcp](https://www.npmjs.com/package/apache-druid-mcp)
- **Apache Druid Community:** [https://druid.apache.org/community/](https://druid.apache.org/community/)