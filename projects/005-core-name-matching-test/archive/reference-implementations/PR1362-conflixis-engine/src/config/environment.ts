import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

export const config = {
  // Elasticsearch Cloud
  elasticsearch: {
    cloudId: process.env.ELASTIC_CLOUD_ID || '',
    apiKeyId: process.env.ELASTIC_API_KEY_ID || '',
    apiKey: process.env.ELASTIC_API_KEY || '',
    indexName: process.env.ES_INDEX_NAME || 'companies',
  },

  // OpenAI
  openai: {
    apiKey: process.env.OPENAI_API_KEY || '',
    model: process.env.OPENAI_MODEL || 'gpt-4-0125-preview',
    temperature: 0.1,
    maxTokens: 100,
    timeout: 10000, // 10 seconds
    // List of allowed models (for validation)
    allowedModels: ['gpt-4o', 'gpt-4o-mini'],
  },

  // API
  api: {
    port: parseInt(process.env.PORT || '8080', 10),
    apiKey: process.env.API_KEY || '',
    environment: process.env.NODE_ENV || 'development',
  },

  // Matching thresholds
  matching: {
    highConfidenceThreshold: 0.95,
    mediumConfidenceThreshold: 0.7,
    lowConfidenceThreshold: 0.3,
    aiEnhancementMinThreshold: 0.7,
    aiEnhancementMaxThreshold: 0.95,
    ambiguityThreshold: 0.15,
  },

  // Performance
  performance: {
    maxResponseTime: {
      noAI: 200, // ms
      withAI: 2000, // ms
      complexDisambiguation: 5000, // ms
    },
    maxConcurrentRequests: 100,
  },

  // Logging
  logging: {
    level: process.env.LOG_LEVEL || 'info',
  },

  // Exact Match Cache
  exactMatchCache: {
    enabled: process.env.EXACT_MATCH_CACHE_ENABLED !== 'false',
    minConfidence: parseFloat(process.env.CACHE_MIN_CONFIDENCE || '0.9'),
    maxEntriesPerCompany: parseInt(process.env.CACHE_MAX_ENTRIES_PER_COMPANY || '50', 10),
    updateBatchSize: parseInt(process.env.CACHE_UPDATE_BATCH_SIZE || '10', 10),
    cacheMatchBoost: parseInt(process.env.CACHE_MATCH_BOOST || '100', 10),
  },
};

// Validate required configuration
export function validateConfig(): void {
  const errors: string[] = [];

  // Elastic Cloud credentials are required
  if (!config.elasticsearch.cloudId) {
    errors.push('ELASTIC_CLOUD_ID is required');
  }
  if (!config.elasticsearch.apiKeyId) {
    errors.push('ELASTIC_API_KEY_ID is required');
  }
  if (!config.elasticsearch.apiKey) {
    errors.push('ELASTIC_API_KEY is required');
  }

  if (!config.openai.apiKey) {
    errors.push('OPENAI_API_KEY is required');
  }

  if (config.api.environment === 'production' && !config.api.apiKey) {
    errors.push('API_KEY is required in production');
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }
}
