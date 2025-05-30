import axios, { AxiosInstance, AxiosResponse } from 'axios';

export interface DruidConfig {
  url: string;
  username?: string;
  password?: string;
  timeout?: number;
}

export interface DruidDatasource {
  name: string;
  properties: {
    created: string;
    segmentGranularity: string;
    type: string;
  };
}

export interface DruidSegment {
  dataSource: string;
  interval: string;
  version: string;
  partition: number;
  size: number;
}

export interface DruidQueryResult {
  data: any[];
  meta?: any;
  context?: any;
}

export interface DruidColumn {
  name: string;
  type: string;
  hasMultipleValues?: boolean;
  size?: number;
  cardinality?: number;
}

export interface DruidDatasourceMetadata {
  id: string;
  intervals: string[];
  columns: DruidColumn[];
  size: number;
  count: number;
  queryGranularity: string;
  segmentGranularity: string;
  rollup: boolean;
}

export class DruidClient {
  private client: AxiosInstance;

  constructor(config: DruidConfig) {
    this.client = axios.create({
      baseURL: config.url.replace(/\/$/, ''), // Remove trailing slash
      timeout: config.timeout || 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add authentication if provided
    if (config.username && config.password) {
      this.client.defaults.auth = {
        username: config.username,
        password: config.password,
      };
    }

    // Add response interceptor for better error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response) {
          const errorMessage = error.response.data?.error || error.response.data?.message || error.message;
          throw new Error(`Druid API Error (${error.response.status}): ${errorMessage}`);
        } else if (error.request) {
          throw new Error(`Network Error: Unable to connect to Druid at ${config.url}`);
        }
        throw error;
      }
    );
  }

  /**
   * Execute a SQL query against Druid
   */
  async executeSqlQuery(query: string, context?: Record<string, any>): Promise<DruidQueryResult> {
    const payload: any = {
      query: query.trim(),
      resultFormat: 'array',
      header: true,
    };

    if (context) {
      payload.context = context;
    }

    const response: AxiosResponse = await this.client.post('/druid/v2/sql', payload);
    
    if (Array.isArray(response.data) && response.data.length > 0) {
      const headers = response.data[0];
      const rows = response.data.slice(1);
      
      return {
        data: rows.map((row: any[]) => {
          const obj: Record<string, any> = {};
          headers.forEach((header: string, index: number) => {
            obj[header] = row[index];
          });
          return obj;
        }),
        meta: { headers },
      };
    }

    return { data: [] };
  }

  /**
   * Execute a native JSON query against Druid
   */
  async executeNativeQuery(query: Record<string, any>): Promise<DruidQueryResult> {
    const response: AxiosResponse = await this.client.post('/druid/v2', query);
    return { data: response.data };
  }

  /**
   * Get list of all datasources
   */
  async getDatasources(): Promise<string[]> {
    const response: AxiosResponse = await this.client.get('/druid/v2/datasources');
    return response.data;
  }

  /**
   * Get detailed metadata for a specific datasource
   */
  async getDatasourceMetadata(datasourceName: string): Promise<DruidDatasourceMetadata> {
    const [metadata, segments] = await Promise.all([
      this.client.get(`/druid/v2/datasources/${datasourceName}`),
      this.getSegments(datasourceName),
    ]);

    const meta = metadata.data;
    
    return {
      id: datasourceName,
      intervals: meta.segments?.intervals || [],
      columns: meta.columns || [],
      size: segments.reduce((total, seg) => total + seg.size, 0),
      count: segments.length,
      queryGranularity: meta.queryGranularity || 'none',
      segmentGranularity: meta.segmentGranularity || 'unknown',
      rollup: meta.rollup || false,
    };
  }

  /**
   * Get segments for a datasource
   */
  async getSegments(datasourceName?: string): Promise<DruidSegment[]> {
    const url = datasourceName 
      ? `/druid/v2/datasources/${datasourceName}/segments`
      : '/druid/v2/segments';
    
    const response: AxiosResponse = await this.client.get(url);
    return response.data;
  }

  /**
   * Get server status
   */
  async getStatus(): Promise<{ status: string; version?: string }> {
    try {
      const response: AxiosResponse = await this.client.get('/status');
      return response.data;
    } catch (error) {
      // Fallback to a simple connectivity check
      try {
        await this.client.get('/druid/v2/datasources');
        return { status: 'healthy' };
      } catch {
        throw new Error('Unable to connect to Druid cluster');
      }
    }
  }

  /**
   * Test connection to Druid
   */
  async testConnection(): Promise<boolean> {
    try {
      await this.getStatus();
      return true;
    } catch {
      return false;
    }
  }
}