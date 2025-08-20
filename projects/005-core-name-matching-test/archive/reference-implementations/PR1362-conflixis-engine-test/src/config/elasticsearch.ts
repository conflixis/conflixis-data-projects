import { Client } from '@elastic/elasticsearch';
import { config } from './environment';

let elasticsearchClient: Client | null = null;

export function getElasticsearchClient(): Client {
  if (!elasticsearchClient) {
    elasticsearchClient = new Client({
      cloud: { id: config.elasticsearch.cloudId },
      auth: {
        apiKey: {
          id: config.elasticsearch.apiKeyId,
          api_key: config.elasticsearch.apiKey,
        },
      },
    });
  }
  return elasticsearchClient;
}

export async function testElasticsearchConnection(): Promise<boolean> {
  try {
    const client = getElasticsearchClient();
    const response = await client.ping();
    return response !== undefined;
  } catch (error) {
    console.error('Elastic Cloud connection test failed:', error);
    return false;
  }
}

export async function ensureCompanyIndex(): Promise<void> {
  const client = getElasticsearchClient();
  const indexName = config.elasticsearch.indexName;

  try {
    const exists = await client.indices.exists({ index: indexName });

    if (!exists) {
      throw new Error(
        `Elasticsearch index '${indexName}' does not exist. Please ensure the index is created before starting the service.`
      );
    }
  } catch (error) {
    console.error('Error checking company index:', error);
    throw error;
  }
}
