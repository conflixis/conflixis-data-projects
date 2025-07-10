import type { SafetyCheckResult } from '@/types';

export class SafetyChecker {
  private forbiddenKeywords = [
    'drop', 'delete', 'truncate', 'alter', 'create',
    'insert', 'update', 'merge', 'grant', 'revoke'
  ];

  private expensivePatterns = [
    /select\s+\*\s+from/i,
    /cross\s+join/i,
    /distinct\s+\*/i,
  ];

  private injectionPatterns = [
    /;\s*drop/i,
    /;\s*delete/i,
    /union\s+select.*null/i,
    /or\s+1\s*=\s*1/i,
    /'\s*or\s*'/i,
    /"\s*or\s*"/i,
    /--\s*$/,
    /\/\*.*\*\//,
  ];

  private maxCostUSD = 1.0;
  private maxRows = 10000;

  async checkQuery(query: string): Promise<SafetyCheckResult> {
    const queryLower = query.toLowerCase();
    const warnings: string[] = [];

    // Check for forbidden keywords
    for (const keyword of this.forbiddenKeywords) {
      if (new RegExp(`\\b${keyword}\\b`, 'i').test(queryLower)) {
        return {
          safe: false,
          reason: `Query contains forbidden keyword: ${keyword}`,
        };
      }
    }

    // Check for expensive patterns
    for (const pattern of this.expensivePatterns) {
      if (pattern.test(query)) {
        warnings.push(`Query matches potentially expensive pattern: ${pattern.source}`);
      }
    }

    // Check for SQL injection patterns
    for (const pattern of this.injectionPatterns) {
      if (pattern.test(query)) {
        return {
          safe: false,
          reason: 'Query contains potential SQL injection patterns',
        };
      }
    }

    // Check for LIMIT clause
    if (!this.hasLimit(queryLower)) {
      warnings.push('Query missing LIMIT clause - will add LIMIT 1000');
    }

    // Check table references
    if (!this.checkTableReferences(query)) {
      return {
        safe: false,
        reason: 'Query references unauthorized tables',
      };
    }

    return {
      safe: true,
      warnings: warnings.length > 0 ? warnings : undefined,
      formattedQuery: query,
    };
  }

  private hasLimit(query: string): boolean {
    return /\blimit\s+\d+/i.test(query);
  }

  private checkTableReferences(query: string): boolean {
    const allowedTables = [
      'data-analytics-389803.conflixis_datasources.op_general_all',
      '`data-analytics-389803.conflixis_datasources.op_general_all`',
      'conflixis_datasources.op_general_all',
      '`conflixis_datasources.op_general_all`'
    ];

    const queryLower = query.toLowerCase();
    return allowedTables.some(table => queryLower.includes(table.toLowerCase()));
  }

  addSafetyLimits(query: string): string {
    if (!this.hasLimit(query.toLowerCase())) {
      const cleanQuery = query.trim().replace(/;?\s*$/, '');
      return `${cleanQuery}\nLIMIT ${this.maxRows}`;
    }
    return query;
  }

  isUnderCostLimit(estimatedCost: number): boolean {
    return estimatedCost <= this.maxCostUSD;
  }
}