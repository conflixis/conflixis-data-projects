/**
 * ElasticsearchService - Handles all interactions with the Elasticsearch database
 *
 * This service is responsible for:
 * - Searching for companies using multiple query strategies (exact, fuzzy, phrase, wildcard)
 * - Retrieving companies by their exact ID
 * - Normalizing Elasticsearch scores to a 0-1 range for consistent confidence scoring
 *
 * The service searches across multiple fields including:
 * - display_name: The primary company name field
 * - submitting_name: Alternative names submitted by users
 * - parent_display_name: Parent company names for subsidiaries
 * - ai_matches: AI-generated name variations and aliases
 *
 * Search strategies are weighted with boost values to prioritize:
 * 1. Exact matches (boost: 10)
 * 2. Phrase matches (boost: 7)
 * 3. Fuzzy matches (boost: 5)
 * 4. Field-specific matches (boost: 3-4)
 * 5. Wildcard partial matches (boost: 2)
 */
import { getElasticsearchClient } from '../config/elasticsearch';
import { config } from '../config/environment';
import { ElasticsearchCompany } from '../types';
import { ExactMatchCacheEntry } from '../types/cache';
import { normalizeCompanyName } from '../utils/normalization';

export class ElasticsearchService {
  private client = getElasticsearchClient();
  private indexName = config.elasticsearch.indexName;

  /**
   * Search for companies using multiple strategies
   */
  async searchCompanies(companyName: string, limit: number = 10): Promise<ElasticsearchCompany[]> {
    const normalizedName = normalizeCompanyName(companyName);

    try {
      const response = await this.client.search({
        index: this.indexName,
        body: {
          size: limit,
          query: {
            bool: {
              should: [
                // Exact match on display_name
                {
                  term: {
                    'display_name.keyword': {
                      value: companyName,
                      boost: 10,
                    },
                  },
                },
                // Fuzzy match on display_name
                {
                  match: {
                    display_name: {
                      query: companyName,
                      fuzziness: 'AUTO',
                      boost: 5,
                    },
                  },
                },
                // Phrase match for multi-word companies
                {
                  match_phrase: {
                    display_name: {
                      query: companyName,
                      slop: 2,
                      boost: 7,
                    },
                  },
                },
                // Match on normalized display_name
                {
                  match: {
                    display_name: {
                      query: normalizedName,
                      fuzziness: 'AUTO',
                      boost: 3,
                    },
                  },
                },
                // Wildcard search for partial matches
                {
                  wildcard: {
                    display_name: {
                      value: `*${normalizedName}*`,
                      boost: 2,
                    },
                  },
                },
                // Also check submitting_name field
                {
                  match: {
                    submitting_name: {
                      query: companyName,
                      fuzziness: 'AUTO',
                      boost: 4,
                    },
                  },
                },
                // Check parent_display_name
                {
                  match: {
                    parent_display_name: {
                      query: companyName,
                      fuzziness: 'AUTO',
                      boost: 3,
                    },
                  },
                },
                // Check ai_matches field for variations
                {
                  match: {
                    ai_matches: {
                      query: companyName,
                      boost: 4,
                    },
                  },
                },
              ],
              minimum_should_match: 1,
            },
          },
          _source: [
            'id',
            'display_name',
            'name',
            'parent_display_name',
            'parent_id',
            'submitting_name',
            'description',
            'country',
            'state',
            'ai_match',
            'ai_matches',
            'aiMatch',
            'match_ids',
            'simkey',
            'is_parent',
            'verified',
            'created_at',
            'updated_at',
            'external_urls',
            'exact_match_cache',
          ],
        },
      });

      return response.hits.hits.map(hit => ({
        ...(hit._source as ElasticsearchCompany),
        _score: hit._score || 0,
      }));
    } catch (error) {
      console.error('Elastic Cloud search error:', error);
      throw new Error('Failed to search companies in Elastic Cloud');
    }
  }

  /**
   * Get company by exact ID
   */
  async getCompanyById(id: string): Promise<ElasticsearchCompany | null> {
    try {
      const response = await this.client.get({
        index: this.indexName,
        id,
      });

      return response._source as ElasticsearchCompany;
    } catch (error) {
      if (error && typeof error === 'object' && 'meta' in error) {
        const errorWithMeta = error as { meta?: { statusCode?: number } };
        if (errorWithMeta.meta?.statusCode === 404) {
          return null;
        }
      }
      throw error;
    }
  }

  /**
   * Calculate normalized Elasticsearch score (0-1 range)
   */
  normalizeScore(score: number, maxScore: number): number {
    if (!score || !maxScore || maxScore === 0) {
      return 0;
    }
    // Use logarithmic scaling for better distribution
    const normalizedScore = Math.log(score + 1) / Math.log(maxScore + 1);
    return Math.max(0, Math.min(1, normalizedScore));
  }

  /**
   * Search for companies by exact match cache
   */
  async searchByExactMatchCache(
    normalizedQuery: string,
    contextHash?: string
  ): Promise<ElasticsearchCompany[]> {
    try {
      const response = await this.client.search({
        index: this.indexName,
        body: {
          size: 10,
          query: {
            bool: {
              must: [
                {
                  term: {
                    'exact_match_cache.normalized_query': {
                      value: normalizedQuery,
                      boost: config.exactMatchCache.cacheMatchBoost,
                    },
                  },
                },
              ],
              filter: contextHash
                ? [
                    {
                      term: {
                        'exact_match_cache.context_hash': contextHash,
                      },
                    },
                  ]
                : undefined,
            },
          },
          _source: true,
        },
      });

      return response.hits.hits.map(hit => hit._source as ElasticsearchCompany);
    } catch (error) {
      console.error('Elastic Cloud cache search error:', error);
      throw new Error('Failed to search cache in Elastic Cloud');
    }
  }

  /**
   * Update a company's exact match cache
   */
  async updateCompanyCache(companyId: string, cacheEntries: ExactMatchCacheEntry[]): Promise<void> {
    try {
      await this.client.update({
        index: this.indexName,
        id: companyId,
        body: {
          doc: {
            exact_match_cache: cacheEntries,
          },
        },
      });
    } catch (error) {
      console.error('Elastic Cloud cache update error:', error);
      throw new Error('Failed to update cache in Elastic Cloud');
    }
  }
}
