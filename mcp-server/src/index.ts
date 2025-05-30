#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { 
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { DruidMCPHandlers } from './mcp-handlers.js';
import { DruidConfig } from './druid-client.js';

/**
 * Get configuration from environment variables
 */
function getConfig(): DruidConfig {
  const url = process.env.DRUID_URL || 'http://localhost:8888';
  const username = process.env.DRUID_USERNAME;
  const password = process.env.DRUID_PASSWORD;
  const timeout = process.env.DRUID_TIMEOUT ? parseInt(process.env.DRUID_TIMEOUT) : 30000;

  return {
    url,
    username,
    password,
    timeout,
  };
}

/**
 * Main server function
 */
async function main() {
  console.error('Starting Apache Druid MCP Server...');

  try {
    // Get configuration
    const config = getConfig();
    console.error(`Connecting to Druid at: ${config.url}`);

    // Initialize handlers
    const handlers = new DruidMCPHandlers(config);

    // Create MCP server
    const server = new Server(
      {
        name: 'apache-druid-mcp',
        version: '1.0.0',
      },
      {
        capabilities: {
          resources: {},
          tools: {},
        },
      }
    );

    // Set up request handlers
    server.setRequestHandler(ListToolsRequestSchema, async (request) => {
      return await handlers.listTools(request);
    });

    server.setRequestHandler(CallToolRequestSchema, async (request) => {
      return await handlers.callTool(request);
    });

    server.setRequestHandler(ListResourcesRequestSchema, async (request) => {
      return await handlers.listResources(request);
    });

    server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      return await handlers.readResource(request);
    });

    // Create transport and connect
    const transport = new StdioServerTransport();
    await server.connect(transport);

    console.error('Apache Druid MCP Server started successfully!');
    console.error('Available environment variables:');
    console.error('  DRUID_URL: Druid broker URL (default: http://localhost:8888)');
    console.error('  DRUID_USERNAME: Optional username for authentication');
    console.error('  DRUID_PASSWORD: Optional password for authentication');
    console.error('  DRUID_TIMEOUT: Request timeout in milliseconds (default: 30000)');
    console.error('');
    console.error('Example usage:');
    console.error('  DRUID_URL=http://your-druid-broker:8888 apache-druid-mcp');
    console.error('');

  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.error('Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.error('Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start the server
if (require.main === module) {
  main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}