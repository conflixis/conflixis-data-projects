"""
API routes for statistics and analytics
"""
from typing import Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends
from api.models import DisclosureStats, HealthCheck
from api.services import DataService


router = APIRouter(prefix="/stats", tags=["statistics"])


def get_data_service() -> DataService:
    """Get data service instance"""
    from .disclosures import get_data_service as get_ds
    return get_ds()


@router.get("/overview", response_model=DisclosureStats)
async def get_overview_stats(
    data_service: DataService = Depends(get_data_service)
):
    """Get overall statistics for all disclosures"""
    return data_service.get_statistics()


@router.get("/metadata")
async def get_metadata(
    data_service: DataService = Depends(get_data_service)
) -> Dict[str, Any]:
    """Get metadata about the data source"""
    return data_service.get_metadata()


@router.get("/health", response_model=HealthCheck)
async def health_check(
    data_service: DataService = Depends(get_data_service)
):
    """Health check endpoint"""
    stats = data_service.get_statistics()
    
    return HealthCheck(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0",
        data_loaded=stats.total_records > 0,
        records_count=stats.total_records
    )


@router.get("/risk-distribution")
async def get_risk_distribution(
    data_service: DataService = Depends(get_data_service)
) -> Dict[str, Any]:
    """Get detailed risk distribution analysis"""
    stats = data_service.get_statistics()
    
    total = stats.total_records
    distribution = stats.risk_distribution
    
    # Calculate percentages
    risk_analysis = {}
    for tier, count in distribution.items():
        percentage = (count / total * 100) if total > 0 else 0
        risk_analysis[tier] = {
            "count": count,
            "percentage": round(percentage, 2),
            "label": tier.replace("_", " ").title()
        }
    
    return {
        "total_records": total,
        "distribution": risk_analysis,
        "highest_risk": max(distribution.items(), key=lambda x: x[1])[0] if distribution else None,
        "management_plans_required": stats.management_plans_required,
        "average_amount": stats.average_amount
    }


@router.get("/provider-summary")
async def get_provider_summary(
    data_service: DataService = Depends(get_data_service)
) -> Dict[str, Any]:
    """Get summary statistics by provider"""
    stats = data_service.get_statistics()
    
    return {
        "unique_providers": stats.unique_providers,
        "unique_entities": stats.unique_entities,
        "average_disclosures_per_provider": (
            stats.total_records / stats.unique_providers 
            if stats.unique_providers > 0 else 0
        ),
        "open_payments_matched": stats.open_payments_matched,
        "match_rate": (
            stats.open_payments_matched / stats.total_records * 100
            if stats.total_records > 0 else 0
        )
    }


@router.get("/compliance-summary")
async def get_compliance_summary(
    data_service: DataService = Depends(get_data_service)
) -> Dict[str, Any]:
    """Get compliance-focused summary statistics"""
    stats = data_service.get_statistics()
    
    # Calculate compliance metrics
    pending_review = stats.review_status_distribution.get('pending', 0)
    approved = stats.review_status_distribution.get('approved', 0)
    rejected = stats.review_status_distribution.get('rejected', 0)
    requires_plan = stats.review_status_distribution.get('requires_management_plan', 0)
    
    return {
        "pending_review": pending_review,
        "approved": approved,
        "rejected": rejected,
        "requires_management_plan": requires_plan,
        "compliance_rate": (
            approved / stats.total_records * 100 
            if stats.total_records > 0 else 0
        ),
        "high_risk_count": (
            stats.risk_distribution.get('high', 0) + 
            stats.risk_distribution.get('critical', 0)
        ),
        "research_disclosures": stats.research_disclosures,
        "financial_summary": {
            "average": stats.average_amount,
            "median": stats.median_amount,
            "maximum": stats.max_amount
        }
    }