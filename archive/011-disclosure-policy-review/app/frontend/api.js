/**
 * API Client for COI Disclosure Review System
 * Automatically detects environment and sets appropriate API endpoint
 */

class DisclosureAPIClient {
    constructor() {
        // Detect environment and set API base URL
        const hostname = window.location.hostname;
        
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
            // Local development
            this.apiBase = 'http://localhost:8000/api';
        } else if (hostname.includes('vercel.app')) {
            // Vercel deployment - update this when you have the API URL
            this.apiBase = process.env.API_URL || 'https://your-api.vercel.app/api';
        } else {
            // Production or other deployment
            this.apiBase = '/api';  // Assumes API is on same domain
        }
        
        console.log(`API Client initialized with base URL: ${this.apiBase}`);
    }
    
    /**
     * Make API request with error handling
     */
    async request(path, options = {}) {
        const url = `${this.apiBase}${path}`;
        
        try {
            const response = await fetch(url, {
                ...options,
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                }
            });
            
            if (!response.ok) {
                throw new Error(`API Error: ${response.status} ${response.statusText}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error('API Request failed:', error);
            throw error;
        }
    }
    
    /**
     * Get paginated disclosures
     */
    async getDisclosures(params = {}) {
        const queryParams = new URLSearchParams(params);
        return this.request(`/disclosures?${queryParams}`);
    }
    
    /**
     * Get specific disclosure by ID
     */
    async getDisclosure(id) {
        return this.request(`/disclosures/${id}`);
    }
    
    /**
     * Search disclosures
     */
    async searchDisclosures(query, page = 1, pageSize = 50) {
        const params = new URLSearchParams({ q: query, page, page_size: pageSize });
        return this.request(`/disclosures/search?${params}`);
    }
    
    /**
     * Get disclosure statistics
     */
    async getStats(filters = {}) {
        const queryParams = new URLSearchParams(filters);
        return this.request(`/disclosures/stats?${queryParams}`);
    }
    
    /**
     * Get overall statistics
     */
    async getOverviewStats() {
        return this.request('/stats/overview');
    }
    
    /**
     * Get risk distribution
     */
    async getRiskDistribution() {
        return this.request('/stats/risk-distribution');
    }
    
    /**
     * Get compliance summary
     */
    async getComplianceSummary() {
        return this.request('/stats/compliance-summary');
    }
    
    /**
     * Get policies
     */
    async getPolicies() {
        return this.request('/policies');
    }
    
    /**
     * Get operational thresholds
     */
    async getThresholds() {
        return this.request('/policies/thresholds');
    }
    
    /**
     * Get full policy configuration
     */
    async getPolicyConfiguration() {
        return this.request('/policies/configuration');
    }
    
    /**
     * Evaluate a disclosure amount
     */
    async evaluateDisclosure(amount) {
        return this.request('/policies/evaluate', {
            method: 'POST',
            body: JSON.stringify({ financial_amount: amount })
        });
    }
    
    /**
     * Get health check
     */
    async healthCheck() {
        return this.request('/stats/health');
    }
    
    /**
     * Reload data from disk
     */
    async reloadData() {
        return this.request('/disclosures/reload', { method: 'POST' });
    }
}

// Export for use in HTML pages
window.DisclosureAPIClient = DisclosureAPIClient;