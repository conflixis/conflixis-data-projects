"""
API routes for disclosure operations
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from api.models import (
    DisclosureRecord, DisclosureFilter, DisclosureResponse,
    DisclosureStats
)
from api.services import DataService


router = APIRouter(prefix="/disclosures", tags=["disclosures"])

# Singleton data service
_data_service: Optional[DataService] = None


def get_data_service() -> DataService:
    """Get or create data service instance"""
    global _data_service
    if _data_service is None:
        _data_service = DataService()
        _data_service.initialize()
    return _data_service


@router.get("", response_model=DisclosureResponse)
async def get_disclosures(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    provider_name: Optional[str] = Query(None, description="Filter by provider name"),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    risk_tier: Optional[str] = Query(None, description="Filter by risk tier"),
    review_status: Optional[str] = Query(None, description="Filter by review status"),
    min_amount: Optional[float] = Query(None, ge=0, description="Minimum financial amount"),
    max_amount: Optional[float] = Query(None, ge=0, description="Maximum financial amount"),
    management_plan_required: Optional[bool] = Query(None, description="Filter by management plan requirement"),
    is_research: Optional[bool] = Query(None, description="Filter by research flag"),
    data_service: DataService = Depends(get_data_service)
):
    """Get paginated list of disclosures with optional filters"""
    
    # Create filter object
    filters = None
    if any([provider_name, entity_name, risk_tier, review_status, 
            min_amount, max_amount, management_plan_required, is_research]):
        filters = DisclosureFilter(
            provider_name=provider_name,
            entity_name=entity_name,
            risk_tier=risk_tier,
            review_status=review_status,
            min_amount=min_amount,
            max_amount=max_amount,
            management_plan_required=management_plan_required,
            is_research=is_research
        )
    
    return data_service.get_disclosures(page=page, page_size=page_size, filters=filters)


@router.get("/stats", response_model=DisclosureStats)
async def get_disclosure_stats(
    provider_name: Optional[str] = Query(None, description="Filter by provider name"),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    risk_tier: Optional[str] = Query(None, description="Filter by risk tier"),
    review_status: Optional[str] = Query(None, description="Filter by review status"),
    min_amount: Optional[float] = Query(None, ge=0, description="Minimum financial amount"),
    max_amount: Optional[float] = Query(None, ge=0, description="Maximum financial amount"),
    data_service: DataService = Depends(get_data_service)
):
    """Get statistics about disclosures with optional filters"""
    
    # Create filter object
    filters = None
    if any([provider_name, entity_name, risk_tier, review_status, min_amount, max_amount]):
        filters = DisclosureFilter(
            provider_name=provider_name,
            entity_name=entity_name,
            risk_tier=risk_tier,
            review_status=review_status,
            min_amount=min_amount,
            max_amount=max_amount
        )
    
    return data_service.get_statistics(filters=filters)


@router.get("/search", response_model=DisclosureResponse)
async def search_disclosures(
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    data_service: DataService = Depends(get_data_service)
):
    """Search disclosures by text query"""
    return data_service.search_disclosures(query=q, page=page, page_size=page_size)


@router.get("/{disclosure_id}", response_model=DisclosureRecord)
async def get_disclosure(
    disclosure_id: str,
    data_service: DataService = Depends(get_data_service)
):
    """Get a specific disclosure by ID"""
    disclosure = data_service.get_disclosure_by_id(disclosure_id)
    
    if disclosure is None:
        raise HTTPException(status_code=404, detail="Disclosure not found")
    
    return disclosure


@router.post("/reload")
async def reload_data(data_service: DataService = Depends(get_data_service)):
    """Force reload data from disk"""
    success = data_service.reload_data()
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to reload data")
    
    return {"message": "Data reloaded successfully", "status": "success"}