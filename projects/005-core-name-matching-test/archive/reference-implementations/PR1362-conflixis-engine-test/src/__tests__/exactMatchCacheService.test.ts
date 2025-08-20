import { config } from '../../config/environment';
import { ElasticsearchCompany } from '../../types';
import { ExactMatchCacheEntry } from '../../types/cache';
import { ElasticsearchService } from '../elasticsearchService';
import { ExactMatchCacheService } from '../exactMatchCacheService';

// Mock the Elasticsearch service
jest.mock('../elasticsearchService');

describe('ExactMatchCacheService', () => {
  let cacheService: ExactMatchCacheService;
  let mockElasticsearchService: jest.Mocked<ElasticsearchService>;

  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();

    // Create mock Elasticsearch service
    mockElasticsearchService = new ElasticsearchService() as jest.Mocked<ElasticsearchService>;

    // Create cache service instance
    cacheService = new ExactMatchCacheService(mockElasticsearchService);

    // Enable cache in config
    config.exactMatchCache.enabled = true;
    config.exactMatchCache.minConfidence = 0.9;
    config.exactMatchCache.maxEntriesPerCompany = 50;
  });

  describe('lookupCache', () => {
    it('should return cache miss when no matches found', async () => {
      mockElasticsearchService.searchByExactMatchCache.mockResolvedValue([]);

      const result = await cacheService.lookupCache('Acme Corp');

      expect(result.hit).toBe(false);
      expect(result.companyId).toBeUndefined();
      expect(result.cacheEntry).toBeUndefined();
    });

    it('should return cache hit when exact match found', async () => {
      const mockCompany: ElasticsearchCompany = {
        id: 'company123',
        display_name: 'Acme Corporation',
        exact_match_cache: [
          {
            query: 'Acme Corp',
            normalized_query: 'acme', // 'Corp' suffix is removed by normalization
            confidence: 0.95,
            match_count: 5,
            last_matched: new Date().toISOString(),
            match_type: 'variation',
          },
        ],
      };

      mockElasticsearchService.searchByExactMatchCache.mockResolvedValue([mockCompany]);

      const result = await cacheService.lookupCache('Acme Corp');

      expect(result.hit).toBe(true);
      expect(result.companyId).toBe('company123');
      expect(result.cacheEntry).toBeDefined();
      expect(result.cacheEntry?.confidence).toBe(0.95);
    });

    it('should handle context hash matching', async () => {
      const context = { industry: 'Technology', region: 'US' };

      await cacheService.lookupCache('Apple Inc', context);

      expect(mockElasticsearchService.searchByExactMatchCache).toHaveBeenCalledWith(
        'apple', // 'Inc' suffix is removed by normalization
        expect.any(String) // Context hash
      );
    });

    it('should track cache statistics', async () => {
      // First call returns no results (cache miss)
      mockElasticsearchService.searchByExactMatchCache.mockResolvedValueOnce([]);

      // Second call returns a matching company (cache hit)
      mockElasticsearchService.searchByExactMatchCache.mockResolvedValueOnce([
        {
          id: 'test-company',
          display_name: 'Hit Company',
          exact_match_cache: [
            {
              query: 'hit',
              normalized_query: 'hit', // This matches the normalized query that will be searched
              confidence: 0.95,
              match_count: 1,
              last_matched: new Date().toISOString(),
            },
          ],
        },
      ]);

      await cacheService.lookupCache('miss');
      await cacheService.lookupCache('hit');

      const stats = cacheService.getStats();
      expect(stats.lookups).toBe(2);
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.hitRate).toBe(0.5);
    });
  });

  describe('updateCache', () => {
    it('should not update cache when disabled', async () => {
      config.exactMatchCache.enabled = false;

      await cacheService.updateCache('company123', 'Test Corp', 0.95, 'exact');

      expect(mockElasticsearchService.getCompanyById).not.toHaveBeenCalled();
      expect(mockElasticsearchService.updateCompanyCache).not.toHaveBeenCalled();
    });

    it('should not update cache when confidence is too low', async () => {
      config.exactMatchCache.minConfidence = 0.9;

      await cacheService.updateCache('company123', 'Test Corp', 0.85, 'fuzzy');

      expect(mockElasticsearchService.getCompanyById).not.toHaveBeenCalled();
      expect(mockElasticsearchService.updateCompanyCache).not.toHaveBeenCalled();
    });

    it('should create new cache entry for company without cache', async () => {
      const mockCompany: ElasticsearchCompany = {
        id: 'company123',
        display_name: 'Test Corporation',
      };

      mockElasticsearchService.getCompanyById.mockResolvedValue(mockCompany);

      await cacheService.updateCache('company123', 'Test Corp', 0.95, 'variation');

      expect(mockElasticsearchService.updateCompanyCache).toHaveBeenCalledWith(
        'company123',
        expect.arrayContaining([
          expect.objectContaining({
            query: 'Test Corp',
            normalized_query: 'test', // 'Corp' suffix is removed by normalization
            confidence: 0.95,
            match_count: 1,
            match_type: 'variation',
          }),
        ])
      );
    });

    it('should update existing cache entry', async () => {
      const existingEntry: ExactMatchCacheEntry = {
        query: 'Test Corp',
        normalized_query: 'test', // 'Corp' suffix is removed by normalization
        confidence: 0.92,
        match_count: 3,
        last_matched: '2024-01-01T00:00:00Z',
        match_type: 'variation',
      };

      const mockCompany: ElasticsearchCompany = {
        id: 'company123',
        display_name: 'Test Corporation',
        exact_match_cache: [existingEntry],
      };

      mockElasticsearchService.getCompanyById.mockResolvedValue(mockCompany);

      await cacheService.updateCache('company123', 'Test Corp', 0.96, 'variation');

      expect(mockElasticsearchService.updateCompanyCache).toHaveBeenCalledWith(
        'company123',
        expect.arrayContaining([
          expect.objectContaining({
            query: 'Test Corp',
            normalized_query: 'test', // 'Corp' suffix is removed by normalization
            confidence: 0.96, // Should be updated to higher confidence
            match_count: 4, // Should be incremented
          }),
        ])
      );
    });

    it('should enforce cache size limit with LRU eviction', async () => {
      config.exactMatchCache.maxEntriesPerCompany = 3;

      // Create cache entries with different match counts and timestamps
      const cacheEntries: ExactMatchCacheEntry[] = [
        {
          query: 'Old Query 1',
          normalized_query: 'old query 1',
          confidence: 0.91,
          match_count: 1,
          last_matched: '2024-01-01T00:00:00Z',
        },
        {
          query: 'Popular Query',
          normalized_query: 'popular query',
          confidence: 0.93,
          match_count: 10,
          last_matched: '2024-01-10T00:00:00Z',
        },
        {
          query: 'Recent Query',
          normalized_query: 'recent query',
          confidence: 0.92,
          match_count: 2,
          last_matched: '2024-01-15T00:00:00Z',
        },
      ];

      const mockCompany: ElasticsearchCompany = {
        id: 'company123',
        display_name: 'Test Corporation',
        exact_match_cache: cacheEntries,
      };

      mockElasticsearchService.getCompanyById.mockResolvedValue(mockCompany);

      await cacheService.updateCache('company123', 'New Query', 0.95, 'exact');

      const updateCall = mockElasticsearchService.updateCompanyCache.mock.calls[0];
      const updatedCache = updateCall[1];

      expect(updatedCache).toHaveLength(3);
      // Old Query 1 should be evicted (lowest match count and oldest)
      expect(updatedCache.find(e => e.query === 'Old Query 1')).toBeUndefined();
      // New Query should be added
      expect(updatedCache.find(e => e.query === 'New Query')).toBeDefined();
    });

    it('should track update statistics', async () => {
      const mockCompany: ElasticsearchCompany = {
        id: 'company123',
        display_name: 'Test Corporation',
      };

      mockElasticsearchService.getCompanyById.mockResolvedValue(mockCompany);

      await cacheService.updateCache('company123', 'Test', 0.95, 'exact');
      await cacheService.updateCache('company123', 'Test Corp', 0.96, 'variation');

      const stats = cacheService.getStats();
      expect(stats.updates).toBe(2);
    });
  });

  describe('getStats and resetStats', () => {
    it('should return current statistics', () => {
      const stats = cacheService.getStats();

      expect(stats).toEqual({
        lookups: 0,
        hits: 0,
        misses: 0,
        hitRate: 0,
        updates: 0,
      });
    });

    it('should reset statistics', async () => {
      // Perform some operations
      mockElasticsearchService.searchByExactMatchCache.mockResolvedValue([]);
      await cacheService.lookupCache('test');

      // Reset stats
      cacheService.resetStats();

      const stats = cacheService.getStats();
      expect(stats.lookups).toBe(0);
      expect(stats.hits).toBe(0);
      expect(stats.misses).toBe(0);
    });
  });
});
