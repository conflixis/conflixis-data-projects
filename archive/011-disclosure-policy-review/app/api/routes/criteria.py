"""
Evaluation Criteria Generation API
Transforms policy interpretations into executable evaluation criteria
"""
import logging
from typing import Dict, List, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Configure logging
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/criteria", tags=["criteria"])

class KeyTermInput(BaseModel):
    """Input model for a key term from interpretation"""
    term: str
    definition: str

class CriteriaRequest(BaseModel):
    """Request model for criteria generation"""
    description: str
    key_terms: List[KeyTermInput]
    exceptions: List[str]
    clause_reference: str = ""

class DataRequirement(BaseModel):
    """Model for data requirements of a component"""
    disclosure_fields: List[str]  # Fields needed from disclosure form
    external_sources: List[str]   # External data sources needed
    derived_fields: List[str] = []  # Fields that need to be calculated/derived

class EvaluationComponent(BaseModel):
    """Model for an evaluation component"""
    type: str
    name: str
    description: str
    config: Dict[str, Any]
    confidence_threshold: float
    data_requirements: DataRequirement  # What data this component needs

class MatchCriteria(BaseModel):
    """Model for match criteria logic"""
    logic: str  # AND, OR, COMPLEX
    conditions: List[str]

class UncertaintyHandling(BaseModel):
    """Model for uncertainty handling rules"""
    partial_match_threshold: float
    manual_review_triggers: List[str]

class CriteriaResponse(BaseModel):
    """Response model for generated criteria"""
    components: List[EvaluationComponent]
    match_criteria: MatchCriteria
    uncertainty_handling: UncertaintyHandling
    required_data_fields: List[str]
    external_data_sources: List[str]

def extract_thresholds(definition: str) -> Dict[str, Any]:
    """Extract numeric thresholds from a key term definition"""
    thresholds = {}
    
    # Extract percentage thresholds (e.g., "≥ 5%", ">= 5%")
    import re
    percent_pattern = r'[≥>=]+\s*(\d+(?:\.\d+)?)\s*%'
    percent_matches = re.findall(percent_pattern, definition)
    if percent_matches:
        thresholds['percentage'] = float(percent_matches[0])
    
    # Extract dollar amounts (e.g., "$50,000", "$1,000,000")
    dollar_pattern = r'\$\s*([\d,]+(?:\.\d+)?)'
    dollar_matches = re.findall(dollar_pattern, definition)
    if dollar_matches:
        # Remove commas and convert to float
        amounts = [float(m.replace(',', '')) for m in dollar_matches]
        if amounts:
            thresholds['amount'] = max(amounts)  # Use the highest amount
    
    # Check for board/officer positions
    if any(term in definition.lower() for term in ['board', 'officer', 'director']):
        thresholds['position'] = True
    
    # Check for critical/major designation
    if any(term in definition.lower() for term in ['critical', 'major', 'designated']):
        thresholds['designation'] = True
    
    return thresholds

def generate_component_for_term(term: str, definition: str) -> EvaluationComponent:
    """Generate an evaluation component based on a key term"""
    term_lower = term.lower()
    thresholds = extract_thresholds(definition)
    
    # Determine component type based on term and definition
    if 'relationship' in term_lower or 'interest' in term_lower:
        return EvaluationComponent(
            type="significance_check",
            name=f"{term} Verification",
            description=f"Check if relationship meets threshold: {definition}",
            config={
                "ownership_threshold": thresholds.get('percentage'),
                "compensation_threshold": thresholds.get('amount'),
                "board_position": thresholds.get('position', False),
                "check_indirect": True
            },
            confidence_threshold=1.0,  # Numeric checks are deterministic
            data_requirements=DataRequirement(
                disclosure_fields=[
                    "entity_name",
                    "ownership_percentage",
                    "ownership_value", 
                    "compensation_amount",
                    "board_positions",
                    "officer_positions"
                ],
                external_sources=[],
                derived_fields=["total_relationship_value"]
            )
        )
    
    elif 'supplier' in term_lower or 'vendor' in term_lower:
        return EvaluationComponent(
            type="supplier_verification",
            name=f"{term} Status Check",
            description=f"Verify supplier status: {definition}",
            config={
                "annual_spend_threshold": thresholds.get('amount'),
                "critical_designation": thresholds.get('designation', False),
                "categories": extract_categories(definition),
                "lookback_months": 12
            },
            confidence_threshold=0.95,
            data_requirements=DataRequirement(
                disclosure_fields=["entity_name", "entity_type", "ein"],
                external_sources=[
                    "vendor_database",
                    "financial_systems",
                    "procurement_records"
                ],
                derived_fields=["annual_spend_total", "supplier_category"]
            )
        )
    
    else:
        # Generic threshold check
        return EvaluationComponent(
            type="threshold_check",
            name=f"{term} Evaluation",
            description=f"Evaluate: {definition}",
            config=thresholds if thresholds else {"custom": True},
            confidence_threshold=0.90,
            data_requirements=DataRequirement(
                disclosure_fields=["entity_name", "relationship_type"],
                external_sources=[],
                derived_fields=[]
            )
        )

def extract_categories(definition: str) -> List[str]:
    """Extract categories from definition (e.g., medical equipment, devices)"""
    categories = []
    
    # Common medical categories
    medical_terms = [
        'medical equipment', 'devices', 'patient services',
        'pharmaceuticals', 'medical supplies', 'diagnostic',
        'therapeutic', 'surgical', 'clinical'
    ]
    
    definition_lower = definition.lower()
    for term in medical_terms:
        if term in definition_lower:
            categories.append(term.replace(' ', '_'))
    
    return categories if categories else ['general']

@router.post("/generate", response_model=CriteriaResponse)
async def generate_evaluation_criteria(request: CriteriaRequest):
    """
    Generate evaluation criteria from interpreted policy
    
    Args:
        request: Interpretation results including key terms and exceptions
        
    Returns:
        Structured evaluation criteria for automated checking
    """
    try:
        components = []
        required_fields = set()
        external_sources = set()
        
        # Always add entity matching as the first component
        components.append(EvaluationComponent(
            type="entity_matching",
            name="Entity Name Matching",
            description="Match disclosed entities against organizational databases",
            config={
                "match_fields": ["name", "ein", "duns_number"],
                "fuzzy_matching": True,
                "alias_resolution": True,
                "min_similarity": 0.75
            },
            confidence_threshold=0.80,
            data_requirements=DataRequirement(
                disclosure_fields=["entity_name", "ein", "duns_number"],
                external_sources=["vendor_database", "entity_registry", "duns_lookup"],
                derived_fields=["normalized_entity_name", "entity_aliases"]
            )
        ))
        required_fields.update(['entity_name', 'ein', 'duns_number'])
        external_sources.add('vendor_database')
        
        # Generate components for each key term
        for key_term in request.key_terms:
            component = generate_component_for_term(key_term.term, key_term.definition)
            components.append(component)
            
            # Determine required fields based on component type
            if component.type == "significance_check":
                required_fields.update([
                    'ownership_percentage', 'ownership_value',
                    'compensation_amount', 'board_positions'
                ])
            elif component.type == "supplier_verification":
                required_fields.update(['entity_type', 'supplier_category'])
                external_sources.update(['vendor_database', 'financial_systems'])
        
        # Add exception handling component if there are exceptions
        if request.exceptions:
            components.append(EvaluationComponent(
                type="exception_check",
                name="Exception Verification",
                description="Check if any policy exceptions apply",
                config={
                    "exceptions": request.exceptions,
                    "check_medical_staff": any('physician' in e.lower() or 'medical staff' in e.lower() 
                                             for e in request.exceptions),
                    "check_de_minimis": any('de minimis' in e.lower() or '1%' in e 
                                          for e in request.exceptions)
                },
                confidence_threshold=0.95,
                data_requirements=DataRequirement(
                    disclosure_fields=["role_type", "medical_staff_status", "ownership_percentage"],
                    external_sources=["medical_staff_roster", "physician_directory"],
                    derived_fields=["is_medical_staff_member", "is_de_minimis_holding"]
                )
            ))
            required_fields.add('role_type')
            external_sources.add('medical_staff_roster')
        
        # Determine match logic based on policy description
        logic = "AND"  # Default to AND logic
        if 'or' in request.description.lower():
            logic = "OR"
        elif len(components) > 3:
            logic = "COMPLEX"
        
        # Build conditions list
        conditions = []
        for component in components:
            if component.type == "entity_matching":
                conditions.append("Entity must be identified with sufficient confidence")
            elif component.type == "significance_check":
                conditions.append("Relationship must meet significance thresholds")
            elif component.type == "supplier_verification":
                conditions.append("Entity must be verified as qualifying supplier")
            elif component.type == "exception_check":
                conditions.append("No applicable exceptions must apply")
        
        # Define uncertainty handling
        uncertainty = UncertaintyHandling(
            partial_match_threshold=0.75,
            manual_review_triggers=[
                "entity_match_confidence < 0.80",
                "missing_critical_fields > 2",
                "conflicting_data_sources",
                "exception_ambiguity"
            ]
        )
        
        return CriteriaResponse(
            components=components,
            match_criteria=MatchCriteria(
                logic=logic,
                conditions=conditions
            ),
            uncertainty_handling=uncertainty,
            required_data_fields=sorted(list(required_fields)),
            external_data_sources=sorted(list(external_sources))
        )
        
    except Exception as e:
        logger.error(f"Error generating criteria: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/test")
async def test_criteria_generation():
    """Test endpoint to verify criteria generation service"""
    return {
        "status": "operational",
        "component_types": [
            "entity_matching",
            "significance_check", 
            "supplier_verification",
            "threshold_check",
            "exception_check"
        ],
        "logic_types": ["AND", "OR", "COMPLEX"]
    }