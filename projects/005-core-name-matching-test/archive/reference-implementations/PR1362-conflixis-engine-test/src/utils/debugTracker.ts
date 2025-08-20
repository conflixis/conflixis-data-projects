import { DebugInfo, TokenUsage } from '../types';

export class DebugTracker {
  private aiCalls = 0;
  private esSearches = 0;
  private esSearchDetails: Array<{ query: string; resultsFound: number; duration: number }> = [];
  private totalTokens: TokenUsage = {
    promptTokens: 0,
    completionTokens: 0,
    totalTokens: 0,
  };
  private aiCallDetails: DebugInfo['aiCallDetails'] = [];
  private enabled: boolean;

  constructor(enabled: boolean = false) {
    this.enabled = enabled;
  }

  trackAICall(functionName: string, tokenUsage: TokenUsage | undefined, duration: number): void {
    if (!this.enabled || !tokenUsage) {
      return;
    }

    this.aiCalls++;
    this.totalTokens.promptTokens += tokenUsage.promptTokens;
    this.totalTokens.completionTokens += tokenUsage.completionTokens;
    this.totalTokens.totalTokens += tokenUsage.totalTokens;

    this.aiCallDetails.push({
      function: functionName,
      tokens: tokenUsage,
      duration,
    });
  }

  trackESSearch(query: string, resultsFound: number, duration: number): void {
    if (!this.enabled) {
      return;
    }

    this.esSearches++;
    this.esSearchDetails.push({
      query,
      resultsFound,
      duration,
    });
  }

  getDebugInfo(): DebugInfo | undefined {
    if (!this.enabled) {
      return undefined;
    }

    return {
      aiCalls: this.aiCalls,
      esSearches: this.esSearches,
      totalTokens: { ...this.totalTokens },
      aiCallDetails: [...this.aiCallDetails],
      esSearchDetails: [...this.esSearchDetails],
    };
  }

  isEnabled(): boolean {
    return this.enabled;
  }
}
