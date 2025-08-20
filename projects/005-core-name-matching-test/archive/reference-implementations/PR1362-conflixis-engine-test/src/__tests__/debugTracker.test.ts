import { TokenUsage } from '../../types';
import { DebugTracker } from '../debugTracker';

describe('DebugTracker', () => {
  describe('when disabled', () => {
    it('should not track any metrics', () => {
      const tracker = new DebugTracker(false);

      const tokenUsage: TokenUsage = {
        promptTokens: 100,
        completionTokens: 50,
        totalTokens: 150,
      };

      tracker.trackAICall('testFunction', tokenUsage, 1000);

      expect(tracker.getDebugInfo()).toBeUndefined();
    });
  });

  describe('when enabled', () => {
    it('should track AI calls and token usage', () => {
      const tracker = new DebugTracker(true);

      const tokenUsage1: TokenUsage = {
        promptTokens: 100,
        completionTokens: 50,
        totalTokens: 150,
      };

      const tokenUsage2: TokenUsage = {
        promptTokens: 200,
        completionTokens: 80,
        totalTokens: 280,
      };

      tracker.trackAICall('evaluateCompanyMatch', tokenUsage1, 1200);
      tracker.trackAICall('checkAbbreviationMatch', tokenUsage2, 800);

      const debugInfo = tracker.getDebugInfo();

      expect(debugInfo).toBeDefined();
      expect(debugInfo?.aiCalls).toBe(2);
      expect(debugInfo?.totalTokens).toEqual({
        promptTokens: 300,
        completionTokens: 130,
        totalTokens: 430,
      });
      expect(debugInfo?.aiCallDetails).toHaveLength(2);
      expect(debugInfo?.aiCallDetails[0]).toEqual({
        function: 'evaluateCompanyMatch',
        tokens: tokenUsage1,
        duration: 1200,
      });
      expect(debugInfo?.aiCallDetails[1]).toEqual({
        function: 'checkAbbreviationMatch',
        tokens: tokenUsage2,
        duration: 800,
      });
    });

    it('should handle undefined token usage', () => {
      const tracker = new DebugTracker(true);

      tracker.trackAICall('testFunction', undefined, 1000);

      const debugInfo = tracker.getDebugInfo();

      expect(debugInfo?.aiCalls).toBe(0);
      expect(debugInfo?.totalTokens).toEqual({
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
      });
      expect(debugInfo?.aiCallDetails).toHaveLength(0);
    });

    it('should correctly report enabled status', () => {
      const enabledTracker = new DebugTracker(true);
      const disabledTracker = new DebugTracker(false);

      expect(enabledTracker.isEnabled()).toBe(true);
      expect(disabledTracker.isEnabled()).toBe(false);
    });
  });
});
