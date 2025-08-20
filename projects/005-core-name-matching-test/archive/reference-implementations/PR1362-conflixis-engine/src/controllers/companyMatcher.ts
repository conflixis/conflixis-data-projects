import { Request, Response } from 'express';
import { MatchingService } from '../services/matchingService';
import { BatchMatchRequest, CompanyMatchRequest } from '../types';
import { sanitizeInput } from '../utils/validation';

export class CompanyMatcherController {
  private matchingService = new MatchingService();

  /**
   * Handle single company match request
   */
  async matchCompany(req: Request, res: Response): Promise<void> {
    const requestId = this.generateRequestId();
    const startTime = Date.now();

    try {
      // Request is already validated by middleware
      const request = req.body as CompanyMatchRequest;

      // Sanitize input
      request.companyName = sanitizeInput(request.companyName);

      console.log(`[${requestId}] Processing match request for: ${request.companyName}`);

      // Perform matching
      const result = await this.matchingService.matchCompany(request);

      // Log performance metrics
      const duration = Date.now() - startTime;
      this.logMetrics(requestId, request.companyName, result.status, duration);

      // Send response
      const response = {
        success: true,
        requestId,
        ...result,
        ...(request.debug && result.debug ? { debug: result.debug } : {}),
      };

      res.status(200).json(response);
    } catch (error) {
      this.handleError(error, res, requestId);
    }
  }

  /**
   * Handle batch company match request
   */
  async batchMatchCompanies(req: Request, res: Response): Promise<void> {
    const requestId = this.generateRequestId();
    const startTime = Date.now();

    try {
      const batchRequest = req.body as BatchMatchRequest;
      console.log(
        `[${requestId}] Processing batch match for ${batchRequest.companies.length} companies`
      );

      const results = await Promise.all(
        batchRequest.companies.map(async company => {
          try {
            // Sanitize input
            company.companyName = sanitizeInput(company.companyName);

            // Use batch model if provided and company doesn't have its own model
            if (batchRequest.model && !company.model) {
              company.model = batchRequest.model;
            }

            const result = await this.matchingService.matchCompany(company);
            return {
              input: company.companyName,
              ...result,
            };
          } catch (error) {
            return {
              input: company.companyName,
              status: 'error',
              message: 'Failed to process this company',
            };
          }
        })
      );

      const duration = Date.now() - startTime;
      console.log(`[${requestId}] Batch processing completed in ${duration}ms`);

      res.status(200).json({
        success: true,
        requestId,
        results,
        processed: results.length,
        duration,
      });
    } catch (error) {
      this.handleError(error, res, requestId);
    }
  }

  /**
   * Health check endpoint
   */
  async healthCheck(req: Request, res: Response): Promise<void> {
    try {
      // Check Elastic Cloud connection
      const { testElasticsearchConnection } = await import('../config/elasticsearch');
      const esHealthy = await testElasticsearchConnection();

      // Get cache stats
      const cacheStats = this.matchingService.getCacheStats();

      res.status(esHealthy ? 200 : 503).json({
        status: esHealthy ? 'healthy' : 'degraded',
        services: {
          elasticCloud: esHealthy ? 'connected' : 'disconnected',
          openai: 'configured', // We don't test OpenAI connection to avoid costs
        },
        cache: cacheStats,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      res.status(503).json({
        status: 'unhealthy',
        error: 'Health check failed',
        timestamp: new Date().toISOString(),
      });
    }
  }

  /**
   * Generate unique request ID
   */
  private generateRequestId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Log performance metrics
   */
  private logMetrics(
    requestId: string,
    companyName: string,
    status: string,
    duration: number
  ): void {
    console.log(
      JSON.stringify({
        type: 'metric',
        requestId,
        companyName: companyName.substring(0, 50), // Truncate for privacy
        status,
        duration,
        timestamp: new Date().toISOString(),
      })
    );
  }

  /**
   * Handle errors consistently
   */
  private handleError(error: unknown, res: Response, requestId: string): void {
    console.error(`[${requestId}] Error:`, error);

    if (error instanceof Error) {
      if (error.name === 'ValidationError') {
        res.status(400).json({
          success: false,
          error: 'Invalid Request',
          message: error.message,
          requestId,
        });
        return;
      }

      if (error.message?.includes('Elastic Cloud')) {
        res.status(503).json({
          success: false,
          error: 'Service Unavailable',
          message: 'Elastic Cloud service is temporarily unavailable',
          requestId,
        });
        return;
      }
    }

    // Generic error response
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      message: 'An unexpected error occurred',
      requestId,
    });
  }
}
