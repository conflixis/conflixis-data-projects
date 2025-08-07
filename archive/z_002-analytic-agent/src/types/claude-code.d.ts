// Type definitions for Claude Code SDK
declare module '@anthropic-ai/claude-code' {
  export interface SDKMessage {
    type: 'assistant' | 'user' | 'system' | 'result';
    message: {
      content: string;
      role?: string;
    };
    timestamp?: string;
  }

  export interface QueryOptions {
    maxTurns?: number;
    allowedTools?: string[];
    systemPrompt?: string;
    workingDirectory?: string;
    permissions?: {
      allow?: string[];
      deny?: string[];
    };
  }

  export interface QueryParams {
    prompt: string;
    abortController?: AbortController;
    cwd?: string;
    executable?: 'node' | 'bun';
    executableArgs?: string[];
    options?: QueryOptions;
  }

  export function query(params: QueryParams): AsyncIterable<SDKMessage>;
}