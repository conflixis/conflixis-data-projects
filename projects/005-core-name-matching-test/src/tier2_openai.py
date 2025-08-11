"""
Tier 2: OpenAI GPT-4 Entity Matching
Uses OpenAI's language model to analyze if two names refer to the same entity
"""

from openai import OpenAI
import json
import os
from typing import Dict, Tuple, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def openai_match(name_a: str, name_b: str, context: Optional[str] = None) -> Tuple[float, Dict]:
    """
    Use OpenAI to analyze if two healthcare organization names refer to the same entity.
    
    Args:
        name_a: First organization name
        name_b: Second organization name
        context: Optional additional context about the organizations
        
    Returns:
        Tuple of (confidence_score, analysis_details)
    """
    
    # Initialize OpenAI client
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("Warning: OPENAI_API_KEY not set. Returning 0 confidence.")
        return 0.0, {"error": "API key not configured"}
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Determine model from environment or use default
        model = os.getenv('TIER2_MODEL', 'gpt-4o-mini')
        
        # Construct the system prompt
        system_prompt = """You are an expert at identifying whether two healthcare organization names refer to the same entity. 

Consider the following factors:
1. Common healthcare abbreviations (hosp., med., ctr., etc.)
2. Parent/subsidiary relationships
3. DBA (doing business as) variations
4. Historical name changes or mergers
5. System affiliations (e.g., "X Hospital" vs "X Health System")
6. Geographic qualifiers that may be added/removed
7. Department or specialty designations

Be conservative - only return high confidence if you're very certain they're the same entity."""

        # Construct the user prompt
        user_prompt = f"""Analyze if these two healthcare organization names refer to the same entity:

Name A: {name_a}
Name B: {name_b}"""

        if context:
            user_prompt += f"\n\nAdditional context: {context}"

        user_prompt += """

Return ONLY a JSON object with the following structure:
{
    "confidence": <0-100 integer representing confidence they are the same entity>,
    "same_entity": <true if same entity, false otherwise>,
    "reasoning": "<brief explanation of your analysis>",
    "entity_type": "<type of healthcare organization if identified>",
    "canonical_name": "<most likely official name if they match>"
}"""

        # Make the API call
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,  # Low temperature for consistency
            max_tokens=300,
            response_format={"type": "json_object"}
        )
        
        # Parse the response
        result = json.loads(response.choices[0].message.content)
        
        # Extract confidence score
        confidence = float(result.get('confidence', 0))
        
        # Add usage information
        result['model_used'] = model
        result['tokens_used'] = response.usage.total_tokens if response.usage else 0
        
        return confidence, result
        
    except json.JSONDecodeError as e:
        print(f"Error parsing OpenAI response: {e}")
        return 0.0, {"error": f"JSON parsing error: {str(e)}"}
        
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return 0.0, {"error": f"API error: {str(e)}"}


def batch_openai_match(name_pairs: list) -> list:
    """
    Process multiple name pairs through OpenAI matching.
    
    Args:
        name_pairs: List of tuples (name_a, name_b)
        
    Returns:
        List of results for each pair
    """
    results = []
    
    for name_a, name_b in name_pairs:
        confidence, details = openai_match(name_a, name_b)
        results.append({
            'name_a': name_a,
            'name_b': name_b,
            'confidence': confidence,
            'details': details
        })
    
    return results


if __name__ == "__main__":
    # Test the OpenAI matching
    test_pairs = [
        ("St. Mary's Hospital", "Saint Marys Medical Center"),
        ("ABC Healthcare LLC", "ABC Health System"),
        ("Johns Hopkins Hospital", "Johns Hopkins Medicine"),
        ("Mayo Clinic", "Mayo Foundation"),
        ("Kaiser Permanente", "Kaiser Foundation Hospitals")
    ]
    
    print("Testing Tier 2 OpenAI Matching:")
    print("-" * 60)
    print("Note: This requires OPENAI_API_KEY to be set in .env file")
    print("-" * 60)
    
    for name_a, name_b in test_pairs:
        print(f"\nAnalyzing: '{name_a}' vs '{name_b}'")
        confidence, details = openai_match(name_a, name_b)
        
        if 'error' in details:
            print(f"  Error: {details['error']}")
        else:
            print(f"  Confidence: {confidence:.1f}%")
            print(f"  Same Entity: {details.get('same_entity', 'Unknown')}")
            print(f"  Reasoning: {details.get('reasoning', 'No reasoning provided')}")
            if details.get('canonical_name'):
                print(f"  Canonical Name: {details['canonical_name']}")