"""
Configurable Evaluation Rules API
Provides flexible rule-based evaluation system for policy compliance
"""
import logging
from typing import Dict, List, Any, Optional, Union
from enum import Enum
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Configure logging
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/evaluation", tags=["evaluation"])

class OperatorType(str, Enum):
    """Supported operators for rule expressions"""
    # Comparison
    EQUALS = "=="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    
    # Membership
    IN = "IN"
    NOT_IN = "NOT_IN"
    CONTAINS = "CONTAINS"
    
    # Logical
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    # Aggregation
    SUM = "SUM"
    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    AVG = "AVG"
    
    # Special
    EXISTS = "EXISTS"
    IS_NULL = "IS_NULL"
    MATCHES = "MATCHES"  # For pattern matching

class DataSourceType(str, Enum):
    """Types of data sources"""
    DISCLOSURE = "disclosure"  # From member's disclosure form
    EXTERNAL = "external"      # From external databases
    DERIVED = "derived"         # Calculated from other fields
    CONSTANT = "constant"       # Fixed values/thresholds

class RuleCondition(BaseModel):
    """A single condition in a rule"""
    field: str                 # e.g., "disclosure.ownership_percentage"
    operator: OperatorType
    value: Optional[Union[str, int, float, bool, List]] = None
    field_type: DataSourceType = DataSourceType.DISCLOSURE

class RuleExpression(BaseModel):
    """A complete rule expression"""
    name: str
    description: str
    conditions: List[RuleCondition]
    logic: OperatorType = OperatorType.AND  # How to combine conditions
    output_variable: str  # Name of the result variable
    confidence_impact: float = 1.0  # How this affects overall confidence

class EvaluationRule(BaseModel):
    """A complete evaluation rule with data requirements"""
    id: str
    name: str
    description: str
    rule_type: str  # e.g., "threshold", "matching", "aggregation"
    
    # The actual rule logic
    expression: RuleExpression
    
    # Data requirements
    required_disclosure_fields: List[str]
    required_external_sources: List[str]
    
    # Configuration
    confidence_threshold: float = 0.8
    manual_review_if_below: bool = True
    
    # Execution order (if dependencies exist)
    depends_on: List[str] = []  # IDs of other rules that must run first

class RuleSet(BaseModel):
    """A collection of rules for a policy"""
    policy_reference: str
    rules: List[EvaluationRule]
    combination_logic: OperatorType = OperatorType.AND
    
class RuleGenerationRequest(BaseModel):
    """Request to generate rules from interpretation"""
    description: str
    key_terms: List[Dict[str, str]]
    exceptions: List[str]
    policy_reference: str

class RuleGenerationResponse(BaseModel):
    """Response with generated rules"""
    rule_set: RuleSet
    data_summary: Dict[str, List[str]]  # Summary of all data requirements

def parse_threshold_to_rule(term: str, definition: str) -> EvaluationRule:
    """Convert a key term with threshold into a rule"""
    import re
    
    # Extract numeric thresholds
    conditions = []
    required_fields = set()
    
    # Check for percentage thresholds
    percent_pattern = r'[≥>=]+\s*(\d+(?:\.\d+)?)\s*%'
    percent_match = re.search(percent_pattern, definition)
    if percent_match:
        threshold = float(percent_match.group(1))
        conditions.append(RuleCondition(
            field="disclosure.ownership_percentage",
            operator=OperatorType.GREATER_EQUAL,
            value=threshold,
            field_type=DataSourceType.DISCLOSURE
        ))
        required_fields.add("ownership_percentage")
    
    # Check for dollar amounts
    dollar_pattern = r'\$\s*([\d,]+(?:\.\d+)?)'
    dollar_match = re.search(dollar_pattern, definition)
    if dollar_match:
        amount = float(dollar_match.group(1).replace(',', ''))
        
        # Determine which field based on context
        if 'compensation' in definition.lower():
            field_name = "disclosure.compensation_amount"
            required_fields.add("compensation_amount")
        elif 'spend' in definition.lower() or 'supplier' in term.lower():
            field_name = "external.annual_spend"
            required_fields.add("annual_spend")
        else:
            field_name = "disclosure.financial_value"
            required_fields.add("financial_value")
            
        conditions.append(RuleCondition(
            field=field_name,
            operator=OperatorType.GREATER_EQUAL,
            value=amount,
            field_type=DataSourceType.EXTERNAL if 'external' in field_name else DataSourceType.DISCLOSURE
        ))
    
    # Check for board/officer positions
    if any(keyword in definition.lower() for keyword in ['board', 'officer', 'director']):
        conditions.append(RuleCondition(
            field="disclosure.has_board_position",
            operator=OperatorType.EQUALS,
            value=True,
            field_type=DataSourceType.DISCLOSURE
        ))
        required_fields.update(["board_positions", "officer_positions"])
    
    # Determine logic (OR if multiple ways to meet threshold)
    logic = OperatorType.OR if len(conditions) > 1 else OperatorType.AND
    
    # Create the rule
    rule_id = term.lower().replace(' ', '_')
    return EvaluationRule(
        id=rule_id,
        name=f"{term} Evaluation",
        description=f"Evaluate if {term} threshold is met: {definition}",
        rule_type="threshold",
        expression=RuleExpression(
            name=f"{term}_check",
            description=definition,
            conditions=conditions,
            logic=logic,
            output_variable=f"meets_{rule_id}_threshold"
        ),
        required_disclosure_fields=list(required_fields),
        required_external_sources=["vendor_database"] if 'supplier' in term.lower() else [],
        confidence_threshold=0.95 if conditions else 0.8
    )

def generate_entity_matching_rule() -> EvaluationRule:
    """Generate the standard entity matching rule"""
    return EvaluationRule(
        id="entity_matching",
        name="Entity Name Matching",
        description="Match disclosed entities against organizational databases",
        rule_type="matching",
        expression=RuleExpression(
            name="entity_match",
            description="Fuzzy match entity names with confidence scoring",
            conditions=[
                RuleCondition(
                    field="disclosure.entity_name",
                    operator=OperatorType.MATCHES,
                    value="external.vendor_name",
                    field_type=DataSourceType.DISCLOSURE
                )
            ],
            logic=OperatorType.AND,
            output_variable="entity_match_confidence",
            confidence_impact=0.8
        ),
        required_disclosure_fields=["entity_name", "ein", "duns_number"],
        required_external_sources=["vendor_database", "entity_registry"],
        confidence_threshold=0.75,
        manual_review_if_below=True
    )

def generate_exception_rules(exceptions: List[str]) -> List[EvaluationRule]:
    """Generate rules for checking exceptions"""
    rules = []
    
    for idx, exception in enumerate(exceptions):
        conditions = []
        required_fields = set()
        
        # Parse the exception to determine what to check
        exception_lower = exception.lower()
        
        if 'physician' in exception_lower or 'medical staff' in exception_lower:
            conditions.append(RuleCondition(
                field="disclosure.role_type",
                operator=OperatorType.EQUALS,
                value="physician",
                field_type=DataSourceType.DISCLOSURE
            ))
            conditions.append(RuleCondition(
                field="external.medical_staff_member",
                operator=OperatorType.EQUALS,
                value=True,
                field_type=DataSourceType.EXTERNAL
            ))
            required_fields.update(["role_type", "medical_license"])
            
        elif 'de minimis' in exception_lower or '1%' in exception:
            conditions.append(RuleCondition(
                field="disclosure.ownership_percentage",
                operator=OperatorType.LESS_THAN,
                value=1.0,
                field_type=DataSourceType.DISCLOSURE
            ))
            required_fields.add("ownership_percentage")
        
        if conditions:
            rules.append(EvaluationRule(
                id=f"exception_{idx + 1}",
                name=f"Exception Check {idx + 1}",
                description=f"Check if exception applies: {exception}",
                rule_type="exception",
                expression=RuleExpression(
                    name=f"exception_{idx + 1}_check",
                    description=exception,
                    conditions=conditions,
                    logic=OperatorType.AND if 'and' in exception_lower else OperatorType.OR,
                    output_variable=f"exception_{idx + 1}_applies",
                    confidence_impact=-1.0  # Negative because exceptions reduce violation likelihood
                ),
                required_disclosure_fields=list(required_fields),
                required_external_sources=["medical_staff_roster"] if 'physician' in exception_lower else [],
                confidence_threshold=0.9
            ))
    
    return rules

@router.post("/generate-rules", response_model=RuleGenerationResponse)
async def generate_evaluation_rules(request: RuleGenerationRequest):
    """
    Generate configurable evaluation rules from policy interpretation
    
    Args:
        request: Interpretation results with key terms and exceptions
        
    Returns:
        Set of configurable evaluation rules
    """
    try:
        rules = []
        all_disclosure_fields = set()
        all_external_sources = set()
        
        # Always start with entity matching
        entity_rule = generate_entity_matching_rule()
        rules.append(entity_rule)
        all_disclosure_fields.update(entity_rule.required_disclosure_fields)
        all_external_sources.update(entity_rule.required_external_sources)
        
        # Generate rules for each key term
        for key_term in request.key_terms:
            term_rule = parse_threshold_to_rule(
                key_term.get("term", ""),
                key_term.get("definition", "")
            )
            rules.append(term_rule)
            all_disclosure_fields.update(term_rule.required_disclosure_fields)
            all_external_sources.update(term_rule.required_external_sources)
        
        # Generate exception rules
        if request.exceptions:
            exception_rules = generate_exception_rules(request.exceptions)
            for exc_rule in exception_rules:
                exc_rule.depends_on = ["entity_matching"]  # Exceptions depend on entity match
                rules.append(exc_rule)
                all_disclosure_fields.update(exc_rule.required_disclosure_fields)
                all_external_sources.update(exc_rule.required_external_sources)
        
        # Create the rule set
        rule_set = RuleSet(
            policy_reference=request.policy_reference,
            rules=rules,
            combination_logic=OperatorType.AND  # All non-exception rules must be true
        )
        
        # Create data summary
        data_summary = {
            "disclosure_fields": sorted(list(all_disclosure_fields)),
            "external_sources": sorted(list(all_external_sources)),
            "derived_fields": [
                "entity_match_confidence",
                "total_relationship_value",
                "exception_applies"
            ]
        }
        
        return RuleGenerationResponse(
            rule_set=rule_set,
            data_summary=data_summary
        )
        
    except Exception as e:
        logger.error(f"Error generating rules: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/evaluate")
async def evaluate_disclosure(
    rule_set: RuleSet,
    disclosure_data: Dict[str, Any],
    external_data: Dict[str, Any]
):
    """
    Evaluate disclosure data against a rule set
    
    This is a placeholder for the actual evaluation engine
    """
    # TODO: Implement the actual rule evaluation engine
    return {
        "status": "evaluation_complete",
        "results": {},
        "confidence": 0.85,
        "manual_review_required": False
    }

@router.get("/operators")
async def get_supported_operators():
    """Get list of supported operators for rule building"""
    return {
        "comparison": [op.value for op in OperatorType if op.value in ["==", "!=", ">", "<", ">=", "<="]],
        "membership": ["IN", "NOT_IN", "CONTAINS"],
        "logical": ["AND", "OR", "NOT"],
        "aggregation": ["SUM", "COUNT", "MAX", "MIN", "AVG"],
        "special": ["EXISTS", "IS_NULL", "MATCHES"]
    }

@router.get("/test")
async def test_evaluation_rules():
    """Test endpoint to verify rule generation service"""
    return {
        "status": "operational",
        "features": [
            "Configurable rule expressions",
            "Multiple operator types",
            "Data source tracking",
            "Dependency management",
            "Confidence scoring"
        ]
    }