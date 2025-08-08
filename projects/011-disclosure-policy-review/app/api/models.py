"""
Pydantic models for COI Disclosure Review System
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class RiskTier(str, Enum):
    """Risk tier classifications"""
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class ReviewStatus(str, Enum):
    """Review status options"""
    PENDING = "pending"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    REQUIRES_MANAGEMENT_PLAN = "requires_management_plan"


class DisclosureRecord(BaseModel):
    """Individual disclosure record"""
    model_config = ConfigDict(from_attributes=True)
    
    id: str
    provider_npi: Optional[str] = None
    provider_name: str
    specialty: Optional[str] = None
    department: Optional[str] = None
    
    entity_name: str
    relationship_type: str
    financial_amount: float = Field(ge=0)
    
    open_payments_total: Optional[float] = Field(default=0, ge=0)
    open_payments_matched: bool = False
    
    review_status: ReviewStatus = ReviewStatus.PENDING
    risk_tier: RiskTier
    risk_score: int = Field(ge=0, le=100)
    
    management_plan_required: bool = False
    recusal_required: bool = False
    
    disclosure_date: str
    relationship_start_date: Optional[str] = None
    relationship_ongoing: bool = True
    last_review_date: Optional[str] = None
    next_review_date: Optional[str] = None
    
    decision_authority_level: str = "staff"
    equity_percentage: float = Field(default=0, ge=0, le=100)
    board_position: bool = False
    
    person_with_interest: Optional[str] = None
    notes: Optional[str] = None
    is_research: bool = False


class DisclosureFilter(BaseModel):
    """Filters for searching disclosures"""
    provider_name: Optional[str] = None
    entity_name: Optional[str] = None
    risk_tier: Optional[RiskTier] = None
    review_status: Optional[ReviewStatus] = None
    min_amount: Optional[float] = Field(default=None, ge=0)
    max_amount: Optional[float] = Field(default=None, ge=0)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    management_plan_required: Optional[bool] = None
    is_research: Optional[bool] = None


class DisclosureResponse(BaseModel):
    """Response for disclosure queries"""
    total: int
    page: int = 1
    page_size: int = 50
    pages: int
    data: List[DisclosureRecord]


class DisclosureStats(BaseModel):
    """Statistics about disclosures"""
    total_records: int
    risk_distribution: Dict[str, int]
    review_status_distribution: Dict[str, int]
    average_amount: float
    median_amount: float
    max_amount: float
    management_plans_required: int
    unique_providers: int
    unique_entities: int
    open_payments_matched: int
    research_disclosures: int


class PolicyRule(BaseModel):
    """Individual policy rule"""
    id: str
    category: str
    description: str
    policy_clause: str
    threshold_min: Optional[float] = None
    threshold_max: Optional[float] = None
    requires_management_plan: bool = False
    requires_recusal: bool = False
    review_level: str = "automated"


class OperationalThreshold(BaseModel):
    """Operational threshold configuration"""
    tier: str
    label: str
    range_min: float
    range_max: float
    description: str
    management_requirements: Dict[str, Any]
    typical_scenarios: List[str]


class PolicyConfiguration(BaseModel):
    """Complete policy configuration"""
    version: str
    last_updated: str
    policies: List[PolicyRule]
    thresholds: List[OperationalThreshold]


class CloudFunctionRequest(BaseModel):
    """Standard request format for cloud functions"""
    method: str
    path: str
    query: Dict[str, Any] = {}
    body: Optional[Dict[str, Any]] = None
    headers: Dict[str, str] = {}


class CloudFunctionResponse(BaseModel):
    """Standard response format for cloud functions"""
    status_code: int = 200
    body: Any
    headers: Dict[str, str] = {"Content-Type": "application/json"}


class HealthCheck(BaseModel):
    """API health check response"""
    status: str = "healthy"
    timestamp: datetime
    version: str = "1.0.0"
    data_loaded: bool = False
    records_count: int = 0