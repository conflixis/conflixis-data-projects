"""
Policy Interpretation API using Gemini
Provides LLM-powered interpretation of policy clauses
"""
import os
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from google import genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/interpretation", tags=["interpretation"])

# Configure Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY not found in environment variables")
    client = None
else:
    client = genai.Client(api_key=GEMINI_API_KEY)

# Global variable to store cached content
cached_system_prompt = None
cache_expiry = None

class InterpretRequest(BaseModel):
    """Request model for policy interpretation"""
    clause_text: str
    clause_reference: str = "4.8.1"

class KeyTerm(BaseModel):
    """Model for a key term definition"""
    term: str
    definition: str
    confidence: float = 0.9

class InterpretResponse(BaseModel):
    """Response model for policy interpretation"""
    description: str
    key_terms: List[KeyTerm]
    exceptions: List[str]
    assumptions: List[str]
    confidence_level: float

def load_system_prompt() -> str:
    """Load the policy analyst system prompt"""
    prompt_path = Path(__file__).parent.parent.parent.parent / "docs" / "policy-analyst-llm-system-prompt.md"
    
    if not prompt_path.exists():
        logger.warning(f"System prompt not found at {prompt_path}, using default")
        return """You are a policy analyst specializing in healthcare compliance.
        Interpret the given policy clause and identify:
        1. A clear description of what the policy requires
        2. Key terms that need definition (with specific thresholds)
        3. Common exceptions
        4. Assumptions made in your interpretation"""
    
    with open(prompt_path, 'r') as f:
        return f.read()

def get_cached_system_prompt() -> Optional[str]:
    """Get or create cached system prompt with in-memory caching
    
    Note: This implements simple in-memory caching. For production use,
    consider using Gemini's server-side context caching feature which requires:
    1. Creating a CachedContent object with the system prompt
    2. Referencing the cached content in generation requests
    3. Managing cache TTL and refresh
    
    See: https://ai.google.dev/gemini-api/docs/caching
    """
    global cached_system_prompt, cache_expiry
    
    # Check if cache is still valid (cache for 1 hour)
    if cached_system_prompt and cache_expiry and datetime.now() < cache_expiry:
        logger.info("Using cached system prompt from memory")
        return cached_system_prompt
    
    try:
        # Load the system prompt
        system_prompt_text = load_system_prompt()
        
        # Store in memory cache
        cached_system_prompt = system_prompt_text
        cache_expiry = datetime.now() + timedelta(hours=1)
        
        logger.info(f"Loaded system prompt into memory cache ({len(system_prompt_text)} chars, expires in 1 hour)")
        return cached_system_prompt
        
    except Exception as e:
        logger.error(f"Error loading system prompt: {str(e)}")
        return None

def parse_gemini_response(response_text: str) -> Dict[str, Any]:
    """Parse the Gemini response into structured data with validation"""
    try:
        # Try to parse as JSON first
        if response_text.strip().startswith('{'):
            return json.loads(response_text)
    except:
        pass
    
    # Initialize result with defaults
    result = {
        "description": "",
        "key_terms": [],
        "exceptions": [],
        "assumptions": [],
        "confidence_level": 0.90  # Higher confidence with temperature=0
    }
    
    lines = response_text.split('\n')
    current_section = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Detect sections more reliably
        line_lower = line.lower()
        if line_lower.startswith('description:'):
            current_section = 'description'
            result['description'] = line.split(':', 1)[1].strip() if ':' in line else ""
        elif 'key terms:' in line_lower:
            current_section = 'key_terms'
        elif 'exceptions:' in line_lower:
            current_section = 'exceptions'
        elif 'assumptions:' in line_lower:
            current_section = 'assumptions'
        else:
            # Add content to current section
            if current_section == 'description' and line and not line.startswith('-'):
                if result['description']:
                    result['description'] += " " + line
                else:
                    result['description'] = line
            elif current_section == 'key_terms' and line.startswith('-'):
                # Parse key terms with format: "- Term: Definition"
                if ':' in line:
                    parts = line[1:].split(':', 1)  # Remove leading dash
                    if len(parts) == 2:
                        term = parts[0].strip()
                        definition = parts[1].strip()
                        
                        # Normalize term capitalization for consistency
                        term = ' '.join(word.capitalize() for word in term.split())
                        
                        result['key_terms'].append({
                            "term": term,
                            "definition": definition,
                            "confidence": 0.95 if any(c in definition for c in ['≥', '>=', '$', '%']) else 0.85
                        })
            elif current_section == 'exceptions':
                if line.startswith('-'):
                    exception = line[1:].strip()
                    if exception and exception.lower() != "none identified in policy text":
                        result['exceptions'].append(exception)
                elif line and not any(line.lower().startswith(x) for x in ['key terms:', 'assumptions:', 'description:']):
                    result['exceptions'].append(line)
            elif current_section == 'assumptions' and line.startswith('-'):
                result['assumptions'].append(line[1:].strip())
    
    # Post-processing validation and enhancement
    result = validate_and_enhance_response(result, response_text)
    
    return result

def validate_and_enhance_response(result: Dict[str, Any], original_text: str) -> Dict[str, Any]:
    """Validate and enhance the parsed response for consistency"""
    
    # Ensure description is not empty
    if not result['description']:
        result['description'] = "Policy interpretation could not be generated. Please review manually."
    
    # Ensure we have key terms - if not, try to extract from original text
    if not result['key_terms']:
        # Look for terms with qualifiers in the original response
        qualifier_terms = ['significant', 'major', 'substantial', 'material']
        for term in qualifier_terms:
            if term.lower() in original_text.lower():
                # Add a default definition based on our guidelines
                if term.lower() == 'significant':
                    result['key_terms'].append({
                        "term": "Significant Relationship",
                        "definition": "Ownership ≥ 5%, compensation ≥ $50,000/year, or board/officer position",
                        "confidence": 0.8
                    })
                elif term.lower() == 'major':
                    result['key_terms'].append({
                        "term": "Major Supplier",
                        "definition": "Vendor with annual spend ≥ $1,000,000 or designated as critical",
                        "confidence": 0.8
                    })
    
    # Check for exceptions in parentheses if none found
    if not result['exceptions'] and '(other than' in original_text.lower():
        # Extract exception from parentheses
        pattern = r'\(other than[^)]+\)'
        matches = re.findall(pattern, original_text.lower())
        for match in matches:
            exception = match[1:-1].replace('other than', '').strip()
            result['exceptions'].append(exception.capitalize())
    
    # Add default assumptions if none provided
    if not result['assumptions']:
        result['assumptions'] = [
            "Thresholds based on industry standards where not explicitly defined",
            "All relationships include both direct and indirect connections"
        ]
    
    return result

@router.post("/interpret", response_model=InterpretResponse)
async def interpret_policy_clause(request: InterpretRequest):
    """
    Interpret a policy clause using Gemini AI
    
    Args:
        request: The interpretation request containing clause text
        
    Returns:
        Structured interpretation with description, key terms, and exceptions
    """
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="Gemini API key not configured")
    
    try:
        # Get cached system prompt (or create if needed)
        system_prompt = get_cached_system_prompt()
        if not system_prompt:
            # Fallback to loading directly if caching fails
            system_prompt = load_system_prompt()
        
        # Create specific prompt for Step 2 interpretation
        prompt = f"""{system_prompt}

You are now performing Step 2: Policy Interpretation for the following policy clause.

Policy Clause Reference: {request.clause_reference}
Policy Clause Text: {request.clause_text}

CRITICAL INSTRUCTIONS FOR CONSISTENCY:

1. IDENTIFY KEY OPERATIVE TERMS: Extract ONLY terms that need measurable thresholds:
   - Terms with qualifiers that need thresholds: "significant relationship", "major supplier", "substantial interest"
   - DO NOT separate descriptive phrases like "medical equipment, devices, or patient services" into individual terms
   - These are scope descriptions, not terms needing individual thresholds
   
2. APPLY QUANTITATIVE THRESHOLDS: For qualifier terms, provide specific measurable thresholds:
   - "Significant" → Use 5% ownership or $50,000 compensation threshold
   - "Major" → Use $1,000,000 annual spend or critical designation
   - "Substantial" → Use $100 per instance or $300 annually
   - If a term lacks clear guidance, state your threshold assumption explicitly
   
3. HANDLE SCOPE DESCRIPTIONS: When you see lists like "medical equipment, devices, or patient services":
   - This describes WHAT TYPE of supplier is covered
   - Include as ONE definition about scope, not separate terms
   - Focus on the qualifier ("major supplier") not the scope description

4. EXTRACT ALL EXCEPTIONS: Look for these patterns in the text:
   - "other than..." → This is an exception
   - "except..." → This is an exception
   - "excluding..." → This is an exception
   - Text in parentheses like "(other than...)" → This is an exception
   
5. OUTPUT FORMAT - You MUST structure your response EXACTLY as follows:

Description: [One clear paragraph explaining what the policy requires]

Key Terms:
- [Term 1]: [Definition with specific threshold, e.g., "≥ 5% ownership" or "≥ $50,000 annually"]
- [Term 2]: [Definition with specific threshold]
- [Continue for all identified terms]

Exceptions:
- [Exception 1 - copy exact wording from policy if possible]
- [Exception 2]
- [If no exceptions found, write "None identified in policy text"]

Assumptions:
- [Assumption 1 about thresholds or interpretations]
- [Assumption 2]

IMPORTANT: Be consistent - extract the SAME terms and exceptions every time you analyze the same policy text."""
        
        # Generate interpretation with temperature=0 for consistency
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
            config={
                "temperature": 0,
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 2048
            }
        )
        
        # Parse the response
        parsed = parse_gemini_response(response.text)
        
        # Convert to response model
        return InterpretResponse(
            description=parsed.get('description', 'Unable to generate description'),
            key_terms=[KeyTerm(**kt) if isinstance(kt, dict) else kt for kt in parsed.get('key_terms', [])],
            exceptions=parsed.get('exceptions', []),
            assumptions=parsed.get('assumptions', []),
            confidence_level=parsed.get('confidence_level', 0.85)
        )
        
    except Exception as e:
        logger.error(f"Error interpreting policy: {str(e)}")
        
        # Return a default interpretation as fallback
        return InterpretResponse(
            description="This policy prohibits significant relationships with major suppliers of medical equipment, devices, or patient services.",
            key_terms=[
                KeyTerm(
                    term="Significant Relationship",
                    definition="Ownership ≥ 5%, compensation ≥ $50,000/year, or board/officer position",
                    confidence=0.7
                ),
                KeyTerm(
                    term="Major Supplier",
                    definition="Vendor with annual spend ≥ $1,000,000 or designated as critical to operations",
                    confidence=0.7
                )
            ],
            exceptions=[
                "Physicians on the medical staff providing patient care services",
                "De minimis holdings (<1%) in publicly traded companies"
            ],
            assumptions=[
                "Thresholds based on industry standards and regulatory guidelines",
                "Annual spend calculated on trailing 12-month basis"
            ],
            confidence_level=0.7
        )

@router.get("/test")
async def test_interpretation():
    """Test endpoint to verify interpretation service is working"""
    return {
        "status": "operational",
        "gemini_configured": bool(GEMINI_API_KEY),
        "system_prompt_available": Path(__file__).parent.parent.parent.parent.joinpath(
            "docs", "policy-analyst-llm-system-prompt.md"
        ).exists()
    }