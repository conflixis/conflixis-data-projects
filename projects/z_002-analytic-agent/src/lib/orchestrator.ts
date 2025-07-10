import { ClaudeClient, ConversationMessage } from './claude-client';
import { BigQueryClient } from './bigquery-client';
import { SafetyChecker } from './safety-checker';
import { getConversationManager } from './conversation-manager';
import type { QueryResponse, WebSocketMessage } from '@/types';
import { readFileSync } from 'fs';
import { join } from 'path';

export class ComplianceOrchestrator {
  private claudeClient?: ClaudeClient;
  private bqClient?: BigQueryClient;
  private safetyChecker: SafetyChecker;
  private systemPrompt: string = '';

  constructor() {
    this.safetyChecker = new SafetyChecker();
  }

  private initialize() {
    if (!this.claudeClient) {
      this.claudeClient = new ClaudeClient();
    }
    if (!this.bqClient) {
      this.bqClient = new BigQueryClient();
    }
    if (!this.systemPrompt) {
      // Load the claude.md content as system prompt
      try {
        const claudeMdPath = join(process.cwd(), 'claude.md');
        this.systemPrompt = readFileSync(claudeMdPath, 'utf-8');
      } catch (error) {
        console.error('Failed to load claude.md:', error);
        this.systemPrompt = '';
      }
    }
  }

  async processQuery(
    userQuery: string,
    onUpdate?: (message: WebSocketMessage) => void
  ): Promise<QueryResponse> {
    this.initialize();
    
    try {
      // Step 1: Generate SQL with Claude
      onUpdate?.({
        type: 'status',
        message: 'Analyzing your compliance question...',
      });

      const sqlGenerationPrompt = `CRITICAL REMINDERS:
- ALL field names are lowercase with underscores
- BOOLEAN fields (use TRUE/FALSE): physician_ownership_indicator, charity_indicator, delay_in_publication_indicator, related_product_indicator
- STRING fields with Yes/No (use "Yes"/"No"): third_party_payment_recipient_indicator, third_party_equals_covered_recipient_indicator
- program_year is STRING - use = "2023" not = 2023
- Use double quotes for STRING values only

User Question: ${userQuery}

Generate a BigQuery SQL query following these rules:
1. Use the exact SQL patterns from the claude.md file
2. physician_ownership_indicator = TRUE (not = "Yes")
3. third_party_payment_recipient_indicator = "Yes" (not = TRUE)
4. Keep the query simple and direct

Respond with:
- Your compliance reasoning (what risks you're looking for)
- The SQL query (wrapped in \`\`\`sql blocks)
- Any caveats or additional queries that might be helpful`;

      const claudeResponse = await this.claudeClient!.generateResponse(
        sqlGenerationPrompt,
        this.systemPrompt
      );

      // Debug logging
      console.log('Claude response preview:', claudeResponse.substring(0, 500));
      console.log('Full Claude response for SQL extraction:', claudeResponse);

      const sqlQuery = this.extractSQL(claudeResponse);
      console.log('Extracted SQL Query:', sqlQuery);
      console.log('SQL Query lines:', sqlQuery.split('\n').map((line, i) => `${i + 1}: ${line}`).join('\n'));
      const complianceReasoning = this.extractReasoning(claudeResponse);

      onUpdate?.({
        type: 'reasoning',
        message: complianceReasoning,
      });

      // Step 2: Safety check and dry run
      onUpdate?.({
        type: 'status',
        message: 'Validating query safety and estimating costs...',
      });

      const safetyResult = await this.safetyChecker.checkQuery(sqlQuery);
      if (!safetyResult.safe) {
        throw new Error(`Query failed safety check: ${safetyResult.reason}`);
      }

      const dryRunResult = await this.bqClient!.dryRun(sqlQuery);
      const estimatedCost = dryRunResult.estimatedCostUSD;

      onUpdate?.({
        type: 'cost_estimate',
        message: `Estimated query cost: $${estimatedCost.toFixed(4)}`,
      });

      if (!this.safetyChecker.isUnderCostLimit(estimatedCost)) {
        throw new Error(`Query exceeds cost limit: $${estimatedCost.toFixed(2)}`);
      }

      // Step 3: Execute query
      onUpdate?.({
        type: 'status',
        message: 'Executing query on BigQuery...',
      });

      const queryResults = await this.bqClient!.executeQuery(sqlQuery);
      
      if (!queryResults.success) {
        throw new Error(queryResults.error || 'Query execution failed');
      }

      // Step 4: Send results to Claude for interpretation
      onUpdate?.({
        type: 'status',
        message: 'Analyzing results for compliance insights...',
      });

      const interpretationPrompt = `You've just run this query to answer a compliance question:

Original Question: ${userQuery}

SQL Query:
${sqlQuery}

Query Results (top rows):
${JSON.stringify(queryResults.rows.slice(0, 50), null, 2)}

Total Rows: ${queryResults.totalRows}

Please provide a comprehensive compliance analysis:
1. Direct answer to the user's question
2. Key compliance risks or red flags identified
3. Patterns that warrant further investigation
4. Recommendations for next steps
5. Additional queries that might provide deeper insights

Remember to think like a senior compliance officer investigating potential FWA.`;

      const finalAnalysis = await this.claudeClient!.generateResponse(
        interpretationPrompt,
        this.systemPrompt
      );

      return {
        success: true,
        query: userQuery,
        sqlGenerated: sqlQuery,
        complianceAnalysis: finalAnalysis,
        resultsPreview: queryResults.rows.slice(0, 10),
        totalRows: queryResults.totalRows,
        queryCost: estimatedCost,
      };

    } catch (error) {
      return {
        success: false,
        query: userQuery,
        error: error instanceof Error ? error.message : 'Unknown error occurred',
      };
    }
  }

  async processConversationQuery(
    userQuery: string,
    conversationId?: string,
    onUpdate?: (message: WebSocketMessage) => void
  ): Promise<QueryResponse & { conversationId: string }> {
    this.initialize();
    
    const conversationManager = getConversationManager();
    
    // Create or retrieve conversation
    let convId = conversationId;
    if (!convId) {
      convId = conversationManager.createConversation({
        sessionType: 'compliance_analysis'
      });
      
      // Add system message to new conversations
      conversationManager.addMessage(convId, {
        role: 'system',
        content: this.systemPrompt
      });
    }
    
    // Get conversation history
    const messages = conversationManager.getMessages(convId);
    
    // Add user query to conversation
    conversationManager.addMessage(convId, {
      role: 'user',
      content: userQuery
    });
    
    try {
      // Step 1: Generate response with conversation context
      onUpdate?.({
        type: 'status',
        message: 'Analyzing your compliance question with conversation context...',
      });

      const sqlGenerationPrompt = `CRITICAL REMINDERS:
- ALL field names are lowercase with underscores
- BOOLEAN fields (use TRUE/FALSE): physician_ownership_indicator, charity_indicator, delay_in_publication_indicator, related_product_indicator
- STRING fields with Yes/No (use "Yes"/"No"): third_party_payment_recipient_indicator, third_party_equals_covered_recipient_indicator
- program_year is STRING - use = "2023" not = 2023
- Use double quotes for STRING values only

User Question: ${userQuery}

Generate a BigQuery SQL query following these rules:
1. Use the exact SQL patterns from the claude.md file
2. physician_ownership_indicator = TRUE (not = "Yes")
3. third_party_payment_recipient_indicator = "Yes" (not = TRUE)
4. Keep the query simple and direct
5. Consider any context from our conversation history

Respond with:
- Your compliance reasoning (what risks you're looking for)
- The SQL query (wrapped in \`\`\`sql blocks)
- Any caveats or additional queries that might be helpful`;

      // Use conversation-aware method
      const { response: claudeResponse } = await this.claudeClient!.generateConversationResponse(
        [...messages, { role: 'user', content: sqlGenerationPrompt }],
        this.systemPrompt,
        2 // Allow 2 turns for clarification if needed
      );

      // Debug logging
      console.log('Claude response preview:', claudeResponse.substring(0, 500));
      console.log('Full Claude response for SQL extraction:', claudeResponse);

      const sqlQuery = this.extractSQL(claudeResponse);
      console.log('Extracted SQL Query:', sqlQuery);
      console.log('SQL Query lines:', sqlQuery.split('\n').map((line, i) => `${i + 1}: ${line}`).join('\n'));
      const complianceReasoning = this.extractReasoning(claudeResponse);

      // Add assistant response to conversation
      conversationManager.addMessage(convId, {
        role: 'assistant',
        content: claudeResponse
      });

      onUpdate?.({
        type: 'reasoning',
        message: complianceReasoning,
      });

      // Rest of the processing remains the same...
      // Step 2: Safety check and dry run
      onUpdate?.({
        type: 'status',
        message: 'Validating query safety and estimating costs...',
      });

      const safetyResult = await this.safetyChecker.checkQuery(sqlQuery);
      if (!safetyResult.safe) {
        throw new Error(`Query failed safety check: ${safetyResult.reason}`);
      }

      const dryRunResult = await this.bqClient!.dryRun(sqlQuery);
      const estimatedCost = dryRunResult.estimatedCostUSD;

      onUpdate?.({
        type: 'cost_estimate',
        message: `Estimated query cost: $${estimatedCost.toFixed(4)}`,
      });

      if (!this.safetyChecker.isUnderCostLimit(estimatedCost)) {
        throw new Error(`Query exceeds cost limit: $${estimatedCost.toFixed(2)}`);
      }

      // Step 3: Execute query
      onUpdate?.({
        type: 'status',
        message: 'Executing query on BigQuery...',
      });

      const queryResults = await this.bqClient!.executeQuery(sqlQuery);
      
      if (!queryResults.success) {
        throw new Error(queryResults.error || 'Query execution failed');
      }

      // Step 4: Send results to Claude for interpretation with conversation context
      onUpdate?.({
        type: 'status',
        message: 'Analyzing results for compliance insights...',
      });

      const interpretationPrompt = `You've just run this query to answer a compliance question. Consider our conversation history when interpreting the results.

Original Question: ${userQuery}

SQL Query:
${sqlQuery}

Query Results (top rows):
${JSON.stringify(queryResults.rows.slice(0, 50), null, 2)}

Total Rows: ${queryResults.totalRows}

Please provide a comprehensive compliance analysis:
1. Direct answer to the user's question
2. Key compliance risks or red flags identified
3. Patterns that warrant further investigation
4. Recommendations for next steps
5. Additional queries that might provide deeper insights

Remember to think like a senior compliance officer investigating potential FWA.`;

      const { response: finalAnalysis } = await this.claudeClient!.generateConversationResponse(
        [...conversationManager.getMessages(convId), { role: 'user', content: interpretationPrompt }],
        this.systemPrompt,
        1
      );

      // Add final analysis to conversation
      conversationManager.addMessage(convId, {
        role: 'assistant',
        content: finalAnalysis
      });

      return {
        success: true,
        query: userQuery,
        sqlGenerated: sqlQuery,
        complianceAnalysis: finalAnalysis,
        resultsPreview: queryResults.rows.slice(0, 10),
        totalRows: queryResults.totalRows,
        queryCost: estimatedCost,
        conversationId: convId
      };

    } catch (error) {
      // Add error to conversation for context
      conversationManager.addMessage(convId, {
        role: 'assistant',
        content: `Error occurred: ${error instanceof Error ? error.message : 'Unknown error'}`
      });

      return {
        success: false,
        query: userQuery,
        error: error instanceof Error ? error.message : 'Unknown error occurred',
        conversationId: convId
      };
    }
  }

  private extractSQL(response: string): string {
    const sqlMatch = response.match(/```sql\n([\s\S]*?)\n```/);
    if (sqlMatch) {
      return sqlMatch[1].trim();
    }
    throw new Error('No SQL query found in response');
  }

  private extractReasoning(response: string): string {
    const sqlIndex = response.indexOf('```sql');
    if (sqlIndex > 0) {
      return response.substring(0, sqlIndex).trim();
    }
    return 'Analyzing for compliance risks...';
  }
}