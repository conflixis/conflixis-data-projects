/**
 * TypeScript API Client for COI Disclosure Review System
 * Integrates with FastAPI backend
 */

import { 
  Disclosure, 
  PaginatedResponse, 
  PolicyConfig, 
  Stats, 
  FilterParams,
  PolicyEvaluation 
} from './types';

class APIError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'APIError';
  }
}

export class DisclosureAPIClient {
  private baseURL: string;

  constructor() {
    // Use environment variable or default to local development
    this.baseURL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api';
  }

  private async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseURL}${path}`;
    
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });

      if (!response.ok) {
        throw new APIError(
          response.status,
          `API Error: ${response.status} ${response.statusText}`
        );
      }

      return await response.json();
    } catch (error) {
      if (error instanceof APIError) {
        throw error;
      }
      console.error('API Request failed:', error);
      throw new Error(`Failed to fetch from ${url}: ${error}`);
    }
  }

  // Disclosure endpoints
  async getDisclosures(params: FilterParams = {}): Promise<PaginatedResponse<Disclosure>> {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        queryParams.append(key, String(value));
      }
    });
    
    const response = await this.request<any>(
      `/disclosures?${queryParams.toString()}`
    );
    
    // Map the API response structure to our expected structure
    return {
      items: response.data || [],
      total: response.total || 0,
      page: response.page || 1,
      page_size: response.page_size || 50,
      pages: response.pages || 0
    };
  }

  async getDisclosure(id: string): Promise<Disclosure> {
    return this.request<Disclosure>(`/disclosures/${id}`);
  }

  async searchDisclosures(
    query: string,
    page = 1,
    pageSize = 50
  ): Promise<PaginatedResponse<Disclosure>> {
    const params = new URLSearchParams({
      q: query,
      page: String(page),
      page_size: String(pageSize),
    });
    
    return this.request<PaginatedResponse<Disclosure>>(
      `/disclosures/search?${params.toString()}`
    );
  }

  // Statistics endpoints
  async getStats(filters: FilterParams = {}): Promise<Stats> {
    const queryParams = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        queryParams.append(key, String(value));
      }
    });
    
    return this.request<Stats>(`/disclosures/stats?${queryParams.toString()}`);
  }

  async getOverviewStats(): Promise<Stats> {
    return this.request<Stats>('/stats/overview');
  }

  async getRiskDistribution(): Promise<{ [key: string]: number }> {
    return this.request<{ [key: string]: number }>('/stats/risk-distribution');
  }

  async getComplianceSummary(): Promise<any> {
    return this.request<any>('/stats/compliance-summary');
  }

  // Policy endpoints
  async getPolicies(): Promise<PolicyConfig> {
    return this.request<PolicyConfig>('/policies');
  }

  async getThresholds(): Promise<PolicyConfig['thresholds']> {
    return this.request<PolicyConfig['thresholds']>('/policies/thresholds');
  }

  async getPolicyConfiguration(): Promise<PolicyConfig> {
    return this.request<PolicyConfig>('/policies/configuration');
  }

  async evaluateDisclosure(amount: number): Promise<PolicyEvaluation> {
    return this.request<PolicyEvaluation>('/policies/evaluate', {
      method: 'POST',
      body: JSON.stringify({ financial_amount: amount }),
    });
  }

  // Utility endpoints
  async healthCheck(): Promise<{ status: string; timestamp: string }> {
    return this.request<{ status: string; timestamp: string }>('/stats/health');
  }

  async reloadData(): Promise<{ message: string; count: number }> {
    return this.request<{ message: string; count: number }>('/disclosures/reload', {
      method: 'POST',
    });
  }
}

// Singleton instance
let apiClient: DisclosureAPIClient | null = null;

export function getAPIClient(): DisclosureAPIClient {
  if (!apiClient) {
    apiClient = new DisclosureAPIClient();
  }
  return apiClient;
}

export default DisclosureAPIClient;