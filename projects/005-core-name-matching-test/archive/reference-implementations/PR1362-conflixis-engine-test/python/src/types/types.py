"""
Type definitions for the PR1362 company matching implementation
Ported from TypeScript to Python
"""

from typing import Dict, List, Optional, TypedDict, Literal, Any
from dataclasses import dataclass
from datetime import datetime


class CompanyMatchRequest(TypedDict, total=False):
    """Request for company matching"""
    companyName: str
    context: Optional[Dict[str, str]]  # industry, region, size
    debug: Optional[bool]
    model: Optional[str]  # gpt-4o, gpt-4o-mini


class CompanyMatch(TypedDict):
    """A single company match result"""
    id: str
    display_name: str
    confidence: float
    matchType: Literal['exact', 'high', 'medium', 'low']
    reasoning: Optional[str]
    confidenceFactors: Optional['ConfidenceFactors']


class ConfidenceFactors(TypedDict, total=False):
    """Factors contributing to confidence score"""
    elasticsearchScore: float
    stringSimilarity: float
    contextMatch: float
    abbreviationMatch: bool
    aiAdjustment: Optional[float]
    typoDetected: bool
    historicalMatch: bool
    corporateStructureMatch: bool


class CompanyMatchResponse(TypedDict):
    """Response from company matching"""
    success: bool
    match: Optional[CompanyMatch]
    alternativeMatches: Optional[List[CompanyMatch]]
    noMatch: Optional[bool]
    debugInfo: Optional['DebugInfo']
    error: Optional[str]


class DebugInfo(TypedDict, total=False):
    """Debug information for tracking performance and costs"""
    totalTokensUsed: int
    estimatedCost: float
    aiCallsMade: int
    processingTimeMs: float
    cacheHit: bool
    elasticsearchTimeMs: float
    aiEnhancementTimeMs: float
    confidenceCalculationSteps: List[str]


class ElasticsearchCompany(TypedDict):
    """Company data from Elasticsearch (or our search)"""
    id: str
    display_name: str
    submitting_name: Optional[str]
    parent_display_name: Optional[str]
    industry: Optional[str]
    region: Optional[str]
    size: Optional[str]
    aliases: Optional[List[str]]
    score: float  # Search relevance score


@dataclass
class CacheEntry:
    """Cache entry for exact match caching"""
    query: str
    normalized_query: str
    result: CompanyMatchResponse
    timestamp: datetime
    hit_count: int = 0


class BatchMatchRequest(TypedDict, total=False):
    """Request for batch company matching"""
    companies: List[CompanyMatchRequest]
    model: Optional[str]
    debug: Optional[bool]


class BatchMatchResponse(TypedDict):
    """Response for batch matching"""
    success: bool
    results: List[CompanyMatchResponse]
    totalTokensUsed: Optional[int]
    totalEstimatedCost: Optional[float]
    processingTimeMs: Optional[float]


# AI Tool Types
class AIToolCall(TypedDict):
    """AI tool function call"""
    name: str
    arguments: Dict[str, Any]
    result: Optional[Any]


class AIEnhancementResult(TypedDict):
    """Result from AI enhancement"""
    adjustedConfidence: float
    reasoning: str
    toolCalls: List[AIToolCall]
    tokensUsed: int


# Configuration Types
class ModelConfig(TypedDict):
    """Configuration for a specific model"""
    name: str
    maxTokens: int
    temperature: float
    costPer1kTokens: float


class MatchingConfig(TypedDict):
    """Configuration for matching service"""
    highConfidenceThreshold: float  # >= this is single match
    mediumConfidenceThreshold: float  # >= this is multiple matches
    fastPathThreshold: float  # >= this skips AI
    aiMinConfidence: float  # Below this, skip AI
    aiMaxConfidence: float  # Above this, skip AI
    cacheEnabled: bool
    cacheTTLSeconds: int
    maxCacheSize: int