import type { SDKMessage } from '@anthropic-ai/claude-code';

export interface ConversationMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

export class ClaudeClient {
  constructor() {
    // Claude Code SDK uses ANTHROPIC_API_KEY environment variable automatically
    const apiKey = process.env.CLAUDE_KEY || process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      throw new Error('CLAUDE_KEY or ANTHROPIC_API_KEY environment variable not set');
    }
    
    // Set the env var if using CLAUDE_KEY
    if (process.env.CLAUDE_KEY && !process.env.ANTHROPIC_API_KEY) {
      process.env.ANTHROPIC_API_KEY = process.env.CLAUDE_KEY;
    }
  }

  async generateResponse(
    prompt: string,
    system?: string,
    maxTokens = 4096,
    temperature = 0.3
  ): Promise<string> {
    try {
      // Dynamic import for ES module
      const { query } = await import('@anthropic-ai/claude-code');
      
      const messages: SDKMessage[] = [];
      
      // Build the full prompt with system context if provided
      const fullPrompt = system 
        ? `${system}\n\nUser request: ${prompt}` 
        : prompt;
      
      // Create abort controller for timeout
      const abortController = new AbortController();
      const timeout = setTimeout(() => abortController.abort(), 120000); // 2 minute timeout
      
      try {
        // Use Claude Code SDK query function
        for await (const message of query({
          prompt: fullPrompt,
          abortController,
          options: {
            maxTurns: 1,  // Single turn for SQL generation
          }
        })) {
          messages.push(message);
          
          // Log for debugging
          console.log('Message type:', message.type);
          if (message.type === 'assistant' && message.message?.content) {
            // Handle Claude Code's content format (array of content blocks)
            const content = this.extractTextFromContent(message.message.content);
            console.log('Assistant message:', content.substring(0, 100) + '...');
          }
        }
        
        // Extract assistant responses
        const assistantMessages = messages
          .filter(m => m.type === 'assistant' && m.message?.content)
          .map(m => this.extractTextFromContent(m.message.content));
        
        const fullResponse = assistantMessages.join('\n');
        
        if (!fullResponse) {
          throw new Error('No response generated from Claude Code');
        }
        
        return fullResponse;
      } finally {
        clearTimeout(timeout);
      }
    } catch (error) {
      console.error('Claude Code API error:', error);
      throw new Error(`Claude Code API error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  async* streamResponse(
    prompt: string,
    system?: string
  ): AsyncGenerator<string, void, unknown> {
    try {
      // Dynamic import for ES module
      const { query } = await import('@anthropic-ai/claude-code');
      
      // Build the full prompt with system context if provided
      const fullPrompt = system 
        ? `${system}\n\nUser request: ${prompt}` 
        : prompt;
      
      const abortController = new AbortController();
      
      // Use Claude Code SDK query function
      for await (const message of query({
        prompt: fullPrompt,
        abortController,
        options: {
          maxTurns: 1,
        }
      })) {
        if (message.type === 'assistant' && message.message?.content) {
          yield this.extractTextFromContent(message.message.content);
        }
      }
    } catch (error) {
      throw new Error(`Claude Code streaming error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  async generateConversationResponse(
    messages: ConversationMessage[],
    systemPrompt?: string,
    maxTurns = 3
  ): Promise<{ response: string; messages: SDKMessage[] }> {
    try {
      // Dynamic import for ES module
      const { query } = await import('@anthropic-ai/claude-code');
      
      const collectedMessages: SDKMessage[] = [];
      
      // Build conversation context
      let conversationPrompt = '';
      
      if (systemPrompt) {
        conversationPrompt += `System: ${systemPrompt}\n\n`;
      }
      
      // Add conversation history
      for (const msg of messages) {
        if (msg.role === 'user') {
          conversationPrompt += `User: ${msg.content}\n\n`;
        } else if (msg.role === 'assistant') {
          conversationPrompt += `Assistant: ${msg.content}\n\n`;
        }
      }
      
      // Add the latest user message if not already included
      const lastMessage = messages[messages.length - 1];
      if (lastMessage?.role === 'user' && !conversationPrompt.endsWith(`User: ${lastMessage.content}\n\n`)) {
        conversationPrompt += `User: ${lastMessage.content}\n\n`;
      }
      
      console.log('Conversation prompt preview:', conversationPrompt.substring(0, 200) + '...');
      
      // Create abort controller for timeout
      const abortController = new AbortController();
      const timeout = setTimeout(() => abortController.abort(), 300000); // 5 minute timeout for multi-turn
      
      try {
        // Use Claude Code SDK query function with multi-turn support
        for await (const message of query({
          prompt: conversationPrompt.trim(),
          abortController,
          options: {
            maxTurns,  // Allow multiple turns
          }
        })) {
          collectedMessages.push(message);
          
          // Log for debugging
          console.log('Message type:', message.type);
          if (message.type === 'assistant' && message.message?.content) {
            const content = this.extractTextFromContent(message.message.content);
            console.log('Assistant message:', content.substring(0, 100) + '...');
          }
        }
        
        // Extract assistant responses
        const assistantMessages = collectedMessages
          .filter(m => m.type === 'assistant' && m.message?.content)
          .map(m => this.extractTextFromContent(m.message.content));
        
        const fullResponse = assistantMessages.join('\n');
        
        if (!fullResponse) {
          throw new Error('No response generated from Claude Code');
        }
        
        return {
          response: fullResponse,
          messages: collectedMessages
        };
      } finally {
        clearTimeout(timeout);
      }
    } catch (error) {
      console.error('Claude Code conversation error:', error);
      throw new Error(`Claude Code conversation error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private extractTextFromContent(content: any): string {
    // Handle Claude Code's content format
    if (Array.isArray(content)) {
      return content
        .filter((block: any) => block.type === 'text')
        .map((block: any) => block.text || '')
        .join('');
    }
    // Fallback for string content
    return String(content);
  }
}