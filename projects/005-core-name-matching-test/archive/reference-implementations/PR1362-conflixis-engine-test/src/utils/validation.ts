import { NextFunction, Request, Response } from 'express';
import { z } from 'zod';
import { config } from '../config/environment';

/**
 * Validate API key if required
 */
export function validateApiKey(req: Request, res: Response, next: NextFunction): void {
  if (config.api.environment === 'production' && config.api.apiKey) {
    const providedKey = req.headers['x-api-key'] || req.query.apiKey;

    if (!providedKey || providedKey !== config.api.apiKey) {
      res.status(401).json({
        error: 'Unauthorized',
        message: 'Invalid or missing API key',
      });
      return;
    }
  }

  next();
}

/**
 * Validate request body against a Zod schema
 */
export function validateRequestBody<T>(schema: z.ZodSchema<T>) {
  return (req: Request, res: Response, next: NextFunction): void => {
    try {
      const result = schema.parse(req.body);
      req.body = result;
      next();
    } catch (error) {
      if (error instanceof z.ZodError) {
        res.status(400).json({
          error: 'Validation Error',
          message: 'Invalid request body',
          details: error.errors.map(err => ({
            path: err.path.join('.'),
            message: err.message,
          })),
        });
        return;
      }

      res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid request body',
      });
    }
  };
}

/**
 * Rate limiting check (simplified version)
 */
const requestCounts = new Map<string, { count: number; resetTime: number }>();

export function rateLimitCheck(req: Request, res: Response, next: NextFunction): void {
  const clientId = req.ip || 'unknown';
  const now = Date.now();
  const windowMs = 60 * 1000; // 1 minute window
  const maxRequests = 100;

  const clientData = requestCounts.get(clientId);

  if (!clientData || now > clientData.resetTime) {
    // Start new window
    requestCounts.set(clientId, {
      count: 1,
      resetTime: now + windowMs,
    });
    next();
    return;
  }

  if (clientData.count >= maxRequests) {
    res.status(429).json({
      error: 'Too Many Requests',
      message: 'Rate limit exceeded. Please try again later.',
      retryAfter: Math.ceil((clientData.resetTime - now) / 1000),
    });
    return;
  }

  clientData.count++;
  next();
}

/**
 * Clean up old rate limit entries periodically
 */
setInterval(() => {
  const now = Date.now();
  for (const [clientId, data] of requestCounts.entries()) {
    if (now > data.resetTime + 60000) {
      requestCounts.delete(clientId);
    }
  }
}, 60000); // Clean up every minute

/**
 * Sanitize user input to prevent injection attacks
 */
export function sanitizeInput(input: string): string {
  return input
    .trim()
    .replace(/[<>]/g, '') // Remove potential HTML tags
    .substring(0, 500); // Limit length
}
