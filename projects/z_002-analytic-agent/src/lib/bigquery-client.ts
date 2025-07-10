import { BigQuery } from '@google-cloud/bigquery';
import type { BigQueryResult, SafetyCheckResult } from '@/types';

export class BigQueryClient {
  private client: BigQuery;
  private projectId: string;
  private datasetId = 'conflixis_datasources';
  private tableId = 'op_general_all';

  constructor() {
    this.projectId = process.env.GOOGLE_CLOUD_PROJECT || 'data-analytics-389803';
    this.client = new BigQuery({
      projectId: this.projectId,
    });
  }

  get fullTableId(): string {
    return `${this.projectId}.${this.datasetId}.${this.tableId}`;
  }

  async dryRun(query: string): Promise<{
    valid: boolean;
    bytesProcessed: number;
    estimatedCostUSD: number;
    error?: string;
  }> {
    try {
      console.log('=== BigQuery dryRun ===');
      console.log('Query:', query);
      console.log('Query lines:');
      query.split('\n').forEach((line, i) => {
        console.log(`${i + 1}: ${line}`);
      });
      
      const options = {
        query,
        dryRun: true,
        location: process.env.BIGQUERY_LOCATION || 'us-east4',
      };

      const [job] = await this.client.createQueryJob(options);
      const bytesProcessed = job.metadata?.statistics?.query?.totalBytesProcessed || 0;
      const gbProcessed = Number(bytesProcessed) / (1024 ** 3);
      const tbProcessed = gbProcessed / 1024;
      
      // $5 per TB after 1TB free tier (simplified calculation)
      const estimatedCostUSD = tbProcessed * 5.0;

      return {
        valid: true,
        bytesProcessed: Number(bytesProcessed),
        estimatedCostUSD: Math.round(estimatedCostUSD * 10000) / 10000,
      };
    } catch (error) {
      return {
        valid: false,
        bytesProcessed: 0,
        estimatedCostUSD: 0,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  async executeQuery(query: string, maxResults = 1000): Promise<BigQueryResult> {
    try {
      console.log('=== BigQuery executeQuery ===');
      console.log('Query:', query);
      console.log('Query lines:');
      query.split('\n').forEach((line, i) => {
        console.log(`${i + 1}: ${line}`);
      });
      
      const options = {
        query,
        location: process.env.BIGQUERY_LOCATION || 'us-east4',
        maxResults,
      };

      const [rows] = await this.client.query(options);
      const [metadata] = await this.client.getJobs();
      
      const jobMetadata = metadata[0]?.metadata;
      const totalRows = jobMetadata?.statistics?.query?.totalRows || rows.length;
      const bytesProcessed = jobMetadata?.statistics?.query?.totalBytesProcessed || 0;
      const cacheHit = jobMetadata?.statistics?.query?.cacheHit || false;

      return {
        success: true,
        rows: rows.map(row => ({ ...row })),
        totalRows: Number(totalRows),
        bytesProcessed: Number(bytesProcessed),
        cacheHit,
      };
    } catch (error) {
      return {
        success: false,
        rows: [],
        totalRows: 0,
        bytesProcessed: 0,
        cacheHit: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  async getTableSchema() {
    const table = this.client.dataset(this.datasetId).table(this.tableId);
    const [metadata] = await table.getMetadata();
    
    return metadata.schema.fields.map((field: any) => ({
      name: field.name,
      type: field.type,
      mode: field.mode || 'NULLABLE',
      description: field.description || '',
    }));
  }

  validateTableReference(query: string): boolean {
    const queryLower = query.toLowerCase();
    const tableRefLower = this.fullTableId.toLowerCase();
    
    return queryLower.includes(tableRefLower) || 
           queryLower.includes(`\`${tableRefLower}\``);
  }
}