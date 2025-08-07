import { NextRequest, NextResponse } from 'next/server';
import { ComplianceOrchestrator } from '@/lib/orchestrator';
import type { QueryRequest, QueryResponse } from '@/types';

export async function POST(request: NextRequest) {
  const orchestrator = new ComplianceOrchestrator();
  try {
    const body = await request.json();
    
    if (!body.query) {
      return NextResponse.json(
        { error: 'Query is required' },
        { status: 400 }
      );
    }

    // Check if we should use conversation mode
    const useConversation = body.conversationId !== undefined || body.enableConversation === true;
    
    let result;
    if (useConversation) {
      // Use the new conversation-aware method
      result = await orchestrator.processConversationQuery(
        body.query,
        body.conversationId
      );
    } else {
      // Use the original single-query method for backward compatibility
      result = await orchestrator.processQuery(body.query);
    }
    
    return NextResponse.json(result);
  } catch (error) {
    console.error('Analysis error:', error);
    return NextResponse.json(
      { 
        success: false,
        query: '',
        error: error instanceof Error ? error.message : 'Internal server error' 
      },
      { status: 500 }
    );
  }
}