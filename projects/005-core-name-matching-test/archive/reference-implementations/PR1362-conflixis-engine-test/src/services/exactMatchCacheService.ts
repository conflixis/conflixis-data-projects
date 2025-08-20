/**
 * ExactMatchCacheService - Manages persistent caching of exact matches in Elasticsearch
 *
 * This service provides:
 * - Fast lookup of previously successful exact matches
 * - Automatic cache population after high-confidence matches
 * - LRU eviction when cache size limits are reached
 * - Cache statistics for monitoring performance
 */
import { createHash } from 'crypto';
import { config } from '../config/environment';
import { ElasticsearchCompany } from '../types';
import { CacheLookupResult, CacheStats, ExactMatchCacheEntry } from '../types/cache';
import { normalizeCompanyName } from '../utils/normalization';
import { ElasticsearchService } from './elasticsearchService';

export class ExactMatchCacheService {
  private elasticsearchService: ElasticsearchService;
  private stats: CacheStats = {
    lookups: 0,
    hits: 0,
    misses: 0,
    hitRate: 0,
    updates: 0,
  };

  constructor(elasticsearchService: ElasticsearchService) {
    this.elasticsearchService = elasticsearchService;
  }

  /**
   * Look up a query in the exact match cache
   */
  async lookupCache(
    query: string,
    context?: { industry?: string; region?: string; size?: string }
  ): Promise<CacheLookupResult> {
    this.stats.lookups++;

    const normalizedQuery = normalizeCompanyName(query);
    const contextHash = context ? this.hashContext(context) : undefined;

    try {
      // Search for companies with this exact match in their cache
      const results = await this.elasticsearchService.searchByExactMatchCache(
        normalizedQuery,
        contextHash
      );

      if (results.length > 0) {
        this.stats.hits++;
        this.updateHitRate();

        // Find the best match (highest confidence, most recent)
        const bestMatch = this.selectBestCacheMatch(results, normalizedQuery, contextHash);

        if (bestMatch) {
          return {
            hit: true,
            companyId: bestMatch.company.id,
            cacheEntry: bestMatch.cacheEntry,
            company: bestMatch.company,
          };
        }
      }
    } catch (error) {
      console.error('[Cache] Error looking up cache:', error);
    }

    this.stats.misses++;
    this.updateHitRate();

    return {
      hit: false,
    };
  }

  /**
   * Update the cache for a company after a successful match
   */
  async updateCache(
    companyId: string,
    query: string,
    confidence: number,
    matchType: string,
    context?: { industry?: string; region?: string; size?: string }
  ): Promise<void> {
    // Only cache high-confidence matches
    if (!config.exactMatchCache?.enabled || confidence < config.exactMatchCache.minConfidence) {
      return;
    }

    this.stats.updates++;

    const normalizedQuery = normalizeCompanyName(query);
    const contextHash = context ? this.hashContext(context) : undefined;

    const newEntry: ExactMatchCacheEntry = {
      query,
      normalized_query: normalizedQuery,
      confidence,
      match_count: 1,
      last_matched: new Date().toISOString(),
      context_hash: contextHash,
      match_type: matchType,
    };

    try {
      // Get current company data
      const company = await this.elasticsearchService.getCompanyById(companyId);
      if (!company) {
        console.error(`[Cache] Company not found for ID: ${companyId}`);
        return;
      }

      // Initialize cache if it doesn't exist
      const currentCache = company.exact_match_cache || [];

      // Check if this query already exists in the cache
      const existingIndex = currentCache.findIndex(
        entry => entry.normalized_query === normalizedQuery && entry.context_hash === contextHash
      );

      if (existingIndex >= 0) {
        // Update existing entry
        currentCache[existingIndex].match_count++;
        currentCache[existingIndex].last_matched = new Date().toISOString();
        currentCache[existingIndex].confidence = Math.max(
          currentCache[existingIndex].confidence,
          confidence
        );
      } else {
        // Add new entry
        currentCache.push(newEntry);

        // Enforce cache size limit with LRU eviction
        if (currentCache.length > config.exactMatchCache.maxEntriesPerCompany) {
          // Sort by match_count (ascending) and last_matched (ascending)
          // to find least recently/frequently used entries
          currentCache.sort((a, b) => {
            if (a.match_count !== b.match_count) {
              return a.match_count - b.match_count;
            }
            return new Date(a.last_matched).getTime() - new Date(b.last_matched).getTime();
          });

          // Remove least used entries
          const entriesToRemove = currentCache.length - config.exactMatchCache.maxEntriesPerCompany;
          currentCache.splice(0, entriesToRemove);
        }
      }

      // Update the company document
      await this.elasticsearchService.updateCompanyCache(companyId, currentCache);

      console.log(
        `[Cache] Updated cache for company ${companyId}: "${query}" (confidence: ${confidence})`
      );
    } catch (error) {
      console.error('[Cache] Error updating cache:', error);
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    return { ...this.stats };
  }

  /**
   * Reset cache statistics
   */
  resetStats(): void {
    this.stats = {
      lookups: 0,
      hits: 0,
      misses: 0,
      hitRate: 0,
      updates: 0,
    };
  }

  /**
   * Select the best cache match from multiple results
   */
  private selectBestCacheMatch(
    companies: ElasticsearchCompany[],
    normalizedQuery: string,
    contextHash?: string
  ): { company: ElasticsearchCompany; cacheEntry: ExactMatchCacheEntry } | null {
    let bestMatch: { company: ElasticsearchCompany; cacheEntry: ExactMatchCacheEntry } | null =
      null;
    let bestScore = 0;

    for (const company of companies) {
      if (!company.exact_match_cache) continue;

      for (const cacheEntry of company.exact_match_cache) {
        if (
          cacheEntry.normalized_query === normalizedQuery &&
          (!contextHash || cacheEntry.context_hash === contextHash)
        ) {
          // Score based on confidence, match count, and recency
          const recencyScore = this.calculateRecencyScore(cacheEntry.last_matched);
          const score =
            cacheEntry.confidence * 0.4 +
            Math.min(cacheEntry.match_count / 10, 1) * 0.3 +
            recencyScore * 0.3;

          if (score > bestScore) {
            bestScore = score;
            bestMatch = { company, cacheEntry };
          }
        }
      }
    }

    return bestMatch;
  }

  /**
   * Calculate a recency score (0-1) based on last matched time
   */
  private calculateRecencyScore(lastMatched: string): number {
    const now = Date.now();
    const matchTime = new Date(lastMatched).getTime();
    const hoursSinceMatch = (now - matchTime) / (1000 * 60 * 60);

    // Full score for matches within 24 hours, decreasing over 30 days
    if (hoursSinceMatch <= 24) return 1;
    if (hoursSinceMatch >= 720) return 0; // 30 days

    return 1 - (hoursSinceMatch - 24) / 696;
  }

  /**
   * Create a hash of the context for consistent comparison
   */
  private hashContext(context: { industry?: string; region?: string; size?: string }): string {
    const contextString = JSON.stringify(
      {
        industry: context.industry || '',
        region: context.region || '',
        size: context.size || '',
      },
      Object.keys(context).sort()
    );

    return createHash('md5').update(contextString).digest('hex').substring(0, 8);
  }

  /**
   * Update the hit rate statistic
   */
  private updateHitRate(): void {
    if (this.stats.lookups > 0) {
      this.stats.hitRate = this.stats.hits / this.stats.lookups;
    }
  }
}
