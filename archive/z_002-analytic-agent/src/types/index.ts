export interface QueryRequest {
  query: string;
  maxResults?: number;
  conversationId?: string;
  enableConversation?: boolean;
}

export interface QueryResponse {
  success: boolean;
  query: string;
  sqlGenerated?: string;
  complianceAnalysis?: string;
  resultsPreview?: any[];
  totalRows?: number;
  queryCost?: number;
  error?: string;
  conversationId?: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  type?: 'message' | 'sql' | 'reasoning' | 'cost' | 'error';
  timestamp: Date;
}

export interface SuggestedQuery {
  title: string;
  description: string;
  query: string;
  complianceFocus: string;
}

export interface WebSocketMessage {
  type: 'status' | 'reasoning' | 'cost_estimate' | 'complete' | 'error';
  message?: string;
  data?: any;
}

export interface ComplianceInsight {
  reasoning: string;
  riskLevel: 'low' | 'medium' | 'high';
  recommendations: string[];
}

export interface BigQueryResult {
  success: boolean;
  rows: Record<string, any>[];
  totalRows: number;
  bytesProcessed: number;
  cacheHit: boolean;
  error?: string;
}

export interface SafetyCheckResult {
  safe: boolean;
  reason?: string;
  warnings?: string[];
  formattedQuery?: string;
}