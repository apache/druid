# Quick Start Guide - Apache Druid MCP Server

Get up and running with the Apache Druid MCP Server in minutes!

## 🚀 Option 1: Claude Desktop (Easiest)

### 1. Install the MCP Server
```bash
npm install -g apache-druid-mcp
```

### 2. Configure Claude Desktop

**macOS:** Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** Edit `%APPDATA%/Claude/claude_desktop_config.json`

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

### 3. Restart Claude Desktop

### 4. Test It Out!
Ask Claude:
- "List all datasources in Druid"
- "Show me the schema for my datasource"
- "Query the top 10 records from wikipedia datasource"

## 🔧 Option 2: VS Code with Continue

### 1. Install MCP Server & Continue Extension
```bash
npm install -g apache-druid-mcp
```
Install Continue extension in VS Code

### 2. Configure Continue
Edit `~/.continue/config.json`:
```json
{
  "models": [
    {
      "title": "Claude 3.5 Sonnet",
      "provider": "anthropic",
      "model": "claude-3-5-sonnet-20241022",
      "apiKey": "your-api-key"
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

### 3. Restart VS Code
Reload the window or restart VS Code

### 4. Start Coding with Druid!
Open Continue panel (Ctrl+Shift+L) and ask about your Druid data

## 🐳 Option 3: Docker (Containerized)

### 1. Using Docker Hub (when available)
```bash
docker run -e DRUID_URL=http://your-druid:8888 apache-druid-mcp
```

### 2. Build from Source
```bash
git clone https://github.com/AnilPuram/druid.git
cd druid/mcp-server
docker build -t apache-druid-mcp .
docker run -e DRUID_URL=http://localhost:8888 apache-druid-mcp
```

### 3. Using Docker Compose
```bash
# Create .env file
echo "DRUID_URL=http://localhost:8888" > .env

# Run
docker-compose up
```

## ⚙️ Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DRUID_URL` | Druid broker URL | `http://localhost:8888` |
| `DRUID_USERNAME` | Authentication username | (none) |
| `DRUID_PASSWORD` | Authentication password | (none) |
| `DRUID_TIMEOUT` | Request timeout (ms) | `30000` |

## 🎯 Example Queries to Try

Once connected, try these with your AI assistant:

**Basic Operations:**
- "List all datasources"
- "Get cluster status"
- "Show me datasource metadata for [your-datasource]"

**SQL Queries:**
- "Get the first 10 rows from wikipedia datasource"
- "Count total records in my datasource"
- "Show me the top pages by view count"

**Time Series Analysis:**
- "Show hourly event counts for the last 24 hours"
- "Get daily aggregations for the past week"
- "Find peak usage times"

**Schema Exploration:**
- "What columns are available in the wikipedia datasource?"
- "Show me the data types of all columns"
- "What's the time range of data available?"

## 🆘 Need Help?

**Common Issues:**
- **Can't find command:** Make sure you ran `npm install -g apache-druid-mcp`
- **Connection failed:** Check your `DRUID_URL` is correct and accessible
- **Authentication error:** Verify `DRUID_USERNAME` and `DRUID_PASSWORD`

**Get Support:**
- 📖 [Full Integration Guide](./INTEGRATION_GUIDE.md)
- 🐛 [GitHub Issues](https://github.com/AnilPuram/druid/issues)
- 📦 [npm Package](https://www.npmjs.com/package/apache-druid-mcp)

## ⚡ Pro Tips

1. **Test Connection First:**
   ```bash
   apache-druid-mcp  # Will show if it can connect
   ```

2. **Use Environment Files:**
   Create `.env` file for consistent configuration
   ```bash
   DRUID_URL=http://localhost:8888
   DRUID_USERNAME=admin
   DRUID_PASSWORD=secret
   ```

3. **Development Mode:**
   ```bash
   export DRUID_URL=http://localhost:8888
   apache-druid-mcp
   ```

4. **Multiple Environments:**
   Configure different Druid environments for dev/staging/prod

---

**That's it!** 🎉 You now have AI-powered access to your Apache Druid data!