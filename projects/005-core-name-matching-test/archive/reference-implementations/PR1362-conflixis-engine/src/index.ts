import cors from 'cors';
import express, { NextFunction, Request, Response } from 'express';
import helmet from 'helmet';
import * as functions from '@google-cloud/functions-framework';
import { ensureCompanyIndex } from './config/elasticsearch';
import { validateConfig } from './config/environment';
import { CompanyMatcherController } from './controllers/companyMatcher';
import { BatchMatchRequestSchema, CompanyMatchRequestSchema } from './types';
import { logger } from './utils/logger';
import { rateLimitCheck, validateApiKey, validateRequestBody } from './utils/validation';

// Initialize the Express app
const app = express();
const controller = new CompanyMatcherController();

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Request logging middleware
app.use((req: Request, res: Response, next: NextFunction) => {
  const startTime = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - startTime;
    logger.logRequest(req.method, req.path, res.statusCode, duration, {
      ip: req.ip,
      userAgent: req.get('user-agent'),
    });
  });

  next();
});

// Routes
app.get('/health', (req, res) => void controller.healthCheck(req, res));

app.post(
  '/match',
  validateApiKey,
  rateLimitCheck,
  validateRequestBody(CompanyMatchRequestSchema),
  (req, res) => void controller.matchCompany(req, res)
);

app.post(
  '/batch',
  validateApiKey,
  rateLimitCheck,
  validateRequestBody(BatchMatchRequestSchema),
  (req, res) => void controller.batchMatchCompanies(req, res)
);

// 404 handler
app.use((req: Request, res: Response) => {
  res.status(404).json({
    error: 'Not Found',
    message: 'The requested endpoint does not exist',
  });
});

// Error handling middleware
app.use((err: Error, req: Request, res: Response, _next: NextFunction) => {
  logger.error('Unhandled error', err);

  res.status(500).json({
    error: 'Internal Server Error',
    message: 'An unexpected error occurred',
  });
});

// Initialize the service
async function initialize(): Promise<void> {
  try {
    // Validate configuration
    validateConfig();
    logger.info('Configuration validated');

    // Ensure Elasticsearch index exists
    await ensureCompanyIndex();
    logger.info('Elasticsearch index verified');

    logger.info('Company matching API initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize service', error as Error);
    throw error;
  }
}

// Initialize on cold start
let initPromise: Promise<void> | null = null;

// Main Cloud Function entry point
functions.http('matchCompanies', async (req: Request, res: Response) => {
  // Ensure initialization on first request
  if (!initPromise) {
    initPromise = initialize();
  }

  try {
    await initPromise;
  } catch (error) {
    logger.error('Initialization failed', error as Error);
    res.status(503).json({
      error: 'Service Unavailable',
      message: 'Service is starting up. Please try again.',
    });
    return;
  }

  // Pass request to Express app
  app(req, res);
});

// Export for local development
export { app };

// Local development server
if (require.main === module) {
  const port = process.env.PORT || 8080;

  initialize()
    .then(() => {
      app.listen(port, () => {
        logger.info(`Company matching API listening on port ${port}`);
        logger.info('Available endpoints:');
        logger.info('  GET  /health - Health check');
        logger.info('  POST /match - Match single company');
        logger.info('  POST /batch - Batch match companies');
      });
    })
    .catch(error => {
      logger.error('Failed to start server', error as Error);
      process.exit(1);
    });
}
