"""
API routes for policy operations
"""
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from api.models import PolicyRule, OperationalThreshold, PolicyConfiguration
from api.services import PolicyService


router = APIRouter(prefix="/policies", tags=["policies"])

# Singleton policy service
_policy_service: Optional[PolicyService] = None


def get_policy_service() -> PolicyService:
    """Get or create policy service instance"""
    global _policy_service
    if _policy_service is None:
        _policy_service = PolicyService()
        _policy_service.initialize()
    return _policy_service


class EvaluationRequest(BaseModel):
    """Request model for disclosure evaluation"""
    financial_amount: float


class EvaluationResponse(BaseModel):
    """Response model for disclosure evaluation"""
    risk_tier: str
    label: str
    description: str
    management_requirements: dict
    typical_scenarios: List[str]


@router.get("", response_model=List[PolicyRule])
async def get_policies(
    policy_service: PolicyService = Depends(get_policy_service)
):
    """Get all policy rules"""
    return policy_service.get_policies()


@router.get("/thresholds", response_model=List[OperationalThreshold])
async def get_thresholds(
    policy_service: PolicyService = Depends(get_policy_service)
):
    """Get all operational thresholds"""
    return policy_service.get_thresholds()


@router.get("/configuration", response_model=PolicyConfiguration)
async def get_policy_configuration(
    policy_service: PolicyService = Depends(get_policy_service)
):
    """Get complete policy configuration including policies and thresholds"""
    return policy_service.get_policy_configuration()


@router.post("/evaluate", response_model=EvaluationResponse)
async def evaluate_disclosure(
    request: EvaluationRequest,
    policy_service: PolicyService = Depends(get_policy_service)
):
    """Evaluate a disclosure amount against policy thresholds"""
    result = policy_service.evaluate_disclosure(request.financial_amount)
    return EvaluationResponse(**result)


@router.get("/clause/{clause}", response_model=PolicyRule)
async def get_policy_by_clause(
    clause: str,
    policy_service: PolicyService = Depends(get_policy_service)
):
    """Get a specific policy by its clause reference"""
    policy = policy_service.get_policy_by_clause(clause)
    
    if policy is None:
        raise HTTPException(status_code=404, detail=f"Policy with clause '{clause}' not found")
    
    return policy