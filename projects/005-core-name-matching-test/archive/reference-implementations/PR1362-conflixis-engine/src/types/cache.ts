/**
 * Types for the exact match caching system
 */
import { ElasticsearchCompany } from './index';

/**
 * Represents a cached exact match entry stored in Elasticsearch
 */
export interface ExactMatchCacheEntry {
  /**
   * The exact search string that was matched
   */
  query: string;

  /**
   * Normalized version of the query for consistent comparison
   */
  normalized_query: string;

  /**
   * The confidence score of the match (0-1)
   */
  confidence: number;

  /**
   * Number of times this query has successfully matched this company
   */
  match_count: number;

  /**
   * ISO timestamp of when this was last matched
   */
  last_matched: string;

  /**
   * Optional hash of the context used for the match
   */
  context_hash?: string;

  /**
   * The match type that was determined
   */
  match_type?: string;
}

/**
 * Configuration for the exact match cache
 */
export interface ExactMatchCacheConfig {
  /**
   * Whether caching is enabled
   */
  enabled: boolean;

  /**
   * Minimum confidence score required to cache a match
   */
  minConfidence: number;

  /**
   * Maximum number of cache entries per company
   */
  maxEntriesPerCompany: number;

  /**
   * Batch size for cache updates
   */
  updateBatchSize: number;

  /**
   * Elasticsearch boost value for cache hits
   */
  cacheMatchBoost: number;
}

/**
 * Result from a cache lookup
 */
export interface CacheLookupResult {
  /**
   * Whether a cache hit was found
   */
  hit: boolean;

  /**
   * The company ID if found
   */
  companyId?: string;

  /**
   * The cached entry that was matched
   */
  cacheEntry?: ExactMatchCacheEntry;

  /**
   * The full company data if found
   */
  company?: ElasticsearchCompany;
}

/**
 * Statistics about cache performance
 */
export interface CacheStats {
  /**
   * Total number of cache lookups
   */
  lookups: number;

  /**
   * Number of cache hits
   */
  hits: number;

  /**
   * Number of cache misses
   */
  misses: number;

  /**
   * Cache hit rate (0-1)
   */
  hitRate: number;

  /**
   * Number of cache updates
   */
  updates: number;
}
