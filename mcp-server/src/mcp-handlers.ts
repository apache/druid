import {
  CallToolRequest,
  CallToolResult,
  ListResourcesRequest,
  ListResourcesResult,
  ListToolsRequest,
  ListToolsResult,
  ReadResourceRequest,
  ReadResourceResult,
  Tool,
  Resource,
} from '@modelcontextprotocol/sdk/types.js';
import { DruidClient, DruidConfig } from './druid-client.js';

export class DruidMCPHandlers {
  private druidClient: DruidClient;

  constructor(config: DruidConfig) {
    this.druidClient = new DruidClient(config);
  }

  /**
   * List available tools
   */
  async listTools(_request: ListToolsRequest): Promise<ListToolsResult> {
    const tools: Tool[] = [
      {
        name: 'execute_sql_query',
        description: 'Execute a SQL query against Apache Druid and return results',
        inputSchema: {
          type: 'object',
          properties: {
            query: {
              type: 'string',
              description: 'SQL query to execute (e.g., SELECT * FROM datasource LIMIT 10)',
            },
            context: {
              type: 'object',
              description: 'Optional query context parameters',
              additionalProperties: true,
            },
          },
          required: ['query'],
        },
      },
      {
        name: 'execute_native_query',
        description: 'Execute a native JSON query against Apache Druid',
        inputSchema: {
          type: 'object',
          properties: {
            query: {
              type: 'object',
              description: 'Native Druid query as JSON object',
            },
          },
          required: ['query'],
        },
      },
      {
        name: 'list_datasources',
        description: 'Get a list of all available datasources in Druid',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'get_datasource_metadata',
        description: 'Get detailed metadata for a specific datasource including schema, size, and segments',
        inputSchema: {
          type: 'object',
          properties: {
            datasource: {
              type: 'string',
              description: 'Name of the datasource to get metadata for',
            },
          },
          required: ['datasource'],
        },
      },
      {
        name: 'get_segments',
        description: 'Get segment information for a datasource or all datasources',
        inputSchema: {
          type: 'object',
          properties: {
            datasource: {
              type: 'string',
              description: 'Optional datasource name to filter segments (omit for all segments)',
            },
          },
        },
      },
      {
        name: 'test_connection',
        description: 'Test connection to the Druid cluster',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
    ];

    return { tools };
  }

  /**
   * Handle tool calls
   */
  async callTool(request: CallToolRequest): Promise<CallToolResult> {
    try {
      switch (request.params.name) {
        case 'execute_sql_query':
          return await this.handleExecuteSqlQuery(request);
        case 'execute_native_query':
          return await this.handleExecuteNativeQuery(request);
        case 'list_datasources':
          return await this.handleListDatasources();
        case 'get_datasource_metadata':
          return await this.handleGetDatasourceMetadata(request);
        case 'get_segments':
          return await this.handleGetSegments(request);
        case 'test_connection':
          return await this.handleTestConnection();
        default:
          throw new Error(`Unknown tool: ${request.params.name}`);
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      return {
        content: [
          {
            type: 'text',
            text: `Error: ${errorMessage}`,
          },
        ],
        isError: true,
      };
    }
  }

  /**
   * List available resources
   */
  async listResources(_request: ListResourcesRequest): Promise<ListResourcesResult> {
    try {
      const datasources = await this.druidClient.getDatasources();
      
      const resources: Resource[] = [
        {
          uri: 'druid://cluster/status',
          name: 'Cluster Status',
          description: 'Current status and health of the Druid cluster',
          mimeType: 'application/json',
        },
        {
          uri: 'druid://datasources',
          name: 'All Datasources',
          description: 'List of all available datasources in the cluster',
          mimeType: 'application/json',
        },
        ...datasources.map((ds) => ({
          uri: `druid://datasource/${ds}`,
          name: `Datasource: ${ds}`,
          description: `Metadata and schema information for datasource '${ds}'`,
          mimeType: 'application/json',
        })),
        ...datasources.map((ds) => ({
          uri: `druid://datasource/${ds}/segments`,
          name: `Segments: ${ds}`,
          description: `Segment information for datasource '${ds}'`,
          mimeType: 'application/json',
        })),
      ];

      return { resources };
    } catch (error) {
      // Return basic resources if we can't connect to Druid
      const resources: Resource[] = [
        {
          uri: 'druid://cluster/status',
          name: 'Cluster Status',
          description: 'Current status and health of the Druid cluster',
          mimeType: 'application/json',
        },
        {
          uri: 'druid://datasources',
          name: 'All Datasources',
          description: 'List of all available datasources in the cluster',
          mimeType: 'application/json',
        },
      ];
      return { resources };
    }
  }

  /**
   * Read a specific resource
   */
  async readResource(request: ReadResourceRequest): Promise<ReadResourceResult> {
    try {
      const uri = request.params.uri;

      if (uri === 'druid://cluster/status') {
        const status = await this.druidClient.getStatus();
        return {
          contents: [
            {
              uri,
              mimeType: 'application/json',
              text: JSON.stringify(status, null, 2),
            },
          ],
        };
      }

      if (uri === 'druid://datasources') {
        const datasources = await this.druidClient.getDatasources();
        return {
          contents: [
            {
              uri,
              mimeType: 'application/json',
              text: JSON.stringify({ datasources }, null, 2),
            },
          ],
        };
      }

      if (uri.startsWith('druid://datasource/')) {
        const parts = uri.split('/');
        const datasourceName = parts[2];

        if (parts.length === 3) {
          // Get datasource metadata
          const metadata = await this.druidClient.getDatasourceMetadata(datasourceName);
          return {
            contents: [
              {
                uri,
                mimeType: 'application/json',
                text: JSON.stringify(metadata, null, 2),
              },
            ],
          };
        } else if (parts[3] === 'segments') {
          // Get segments for datasource
          const segments = await this.druidClient.getSegments(datasourceName);
          return {
            contents: [
              {
                uri,
                mimeType: 'application/json',
                text: JSON.stringify({ segments }, null, 2),
              },
            ],
          };
        }
      }

      throw new Error(`Unknown resource URI: ${uri}`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      return {
        contents: [
          {
            uri: request.params.uri,
            mimeType: 'text/plain',
            text: `Error reading resource: ${errorMessage}`,
          },
        ],
      };
    }
  }

  // Private helper methods for tool handlers

  private async handleExecuteSqlQuery(request: CallToolRequest): Promise<CallToolResult> {
    const { query, context } = request.params.arguments as {
      query: string;
      context?: Record<string, any>;
    };

    const result = await this.druidClient.executeSqlQuery(query, context);
    
    return {
      content: [
        {
          type: 'text',
          text: `Query executed successfully. Returned ${result.data.length} rows.\n\n` +
                `Results:\n${JSON.stringify(result.data, null, 2)}`,
        },
      ],
    };
  }

  private async handleExecuteNativeQuery(request: CallToolRequest): Promise<CallToolResult> {
    const { query } = request.params.arguments as { query: Record<string, any> };

    const result = await this.druidClient.executeNativeQuery(query);
    
    return {
      content: [
        {
          type: 'text',
          text: `Native query executed successfully.\n\n` +
                `Results:\n${JSON.stringify(result.data, null, 2)}`,
        },
      ],
    };
  }

  private async handleListDatasources(): Promise<CallToolResult> {
    const datasources = await this.druidClient.getDatasources();
    
    return {
      content: [
        {
          type: 'text',
          text: `Found ${datasources.length} datasources:\n\n` +
                datasources.map((ds, i) => `${i + 1}. ${ds}`).join('\n'),
        },
      ],
    };
  }

  private async handleGetDatasourceMetadata(request: CallToolRequest): Promise<CallToolResult> {
    const { datasource } = request.params.arguments as { datasource: string };

    const metadata = await this.druidClient.getDatasourceMetadata(datasource);
    
    return {
      content: [
        {
          type: 'text',
          text: `Metadata for datasource '${datasource}':\n\n` +
                JSON.stringify(metadata, null, 2),
        },
      ],
    };
  }

  private async handleGetSegments(request: CallToolRequest): Promise<CallToolResult> {
    const { datasource } = request.params.arguments as { datasource?: string };

    const segments = await this.druidClient.getSegments(datasource);
    
    const title = datasource 
      ? `Segments for datasource '${datasource}'`
      : 'All segments in cluster';
    
    return {
      content: [
        {
          type: 'text',
          text: `${title}:\n\nFound ${segments.length} segments.\n\n` +
                JSON.stringify(segments, null, 2),
        },
      ],
    };
  }

  private async handleTestConnection(): Promise<CallToolResult> {
    const isConnected = await this.druidClient.testConnection();
    
    if (isConnected) {
      const status = await this.druidClient.getStatus();
      return {
        content: [
          {
            type: 'text',
            text: `✅ Successfully connected to Druid cluster!\n\n` +
                  `Status: ${JSON.stringify(status, null, 2)}`,
          },
        ],
      };
    } else {
      return {
        content: [
          {
            type: 'text',
            text: `❌ Failed to connect to Druid cluster. Please check your configuration.`,
          },
        ],
        isError: true,
      };
    }
  }
}