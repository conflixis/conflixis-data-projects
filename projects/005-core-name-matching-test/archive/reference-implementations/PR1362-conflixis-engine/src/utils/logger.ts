import { config } from '../config/environment';

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

class Logger {
  private levels: Record<LogLevel, number> = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
  };

  private currentLevel: number;

  constructor() {
    this.currentLevel = this.levels[config.logging.level as LogLevel] || this.levels.info;
  }

  private shouldLog(level: LogLevel): boolean {
    return this.levels[level] >= this.currentLevel;
  }

  private formatMessage(level: LogLevel, message: string, meta?: Record<string, unknown>): string {
    const timestamp = new Date().toISOString();
    const baseLog = {
      timestamp,
      level,
      message,
      ...meta,
    };
    return JSON.stringify(baseLog);
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('debug')) {
      console.debug(this.formatMessage('debug', message, meta));
    }
  }

  info(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('info')) {
      console.info(this.formatMessage('info', message, meta));
    }
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    if (this.shouldLog('warn')) {
      console.warn(this.formatMessage('warn', message, meta));
    }
  }

  error(message: string, error?: Error, meta?: Record<string, unknown>): void {
    if (this.shouldLog('error')) {
      const errorMeta = {
        ...meta,
      };
      if (error) {
        Object.assign(errorMeta, {
          error: {
            message: error.message,
            stack: error.stack,
            name: error.name,
          },
        });
      }
      console.error(this.formatMessage('error', message, errorMeta));
    }
  }

  // Log API requests
  logRequest(
    method: string,
    path: string,
    statusCode: number,
    duration: number,
    meta?: Record<string, unknown>
  ): void {
    this.info('API Request', {
      method,
      path,
      statusCode,
      duration,
      ...meta,
    });
  }

  // Log external service calls
  logServiceCall(
    service: string,
    operation: string,
    duration: number,
    success: boolean,
    meta?: Record<string, unknown>
  ): void {
    const level = success ? 'info' : 'warn';
    this[level](`External service call: ${service}`, {
      service,
      operation,
      duration,
      success,
      ...meta,
    });
  }
}

export const logger = new Logger();
