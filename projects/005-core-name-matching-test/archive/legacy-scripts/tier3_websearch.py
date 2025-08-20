"""
Tier 3: OpenAI with Web Search Validation
Uses OpenAI GPT-4o to search the web and validate entity matching
"""

from openai import OpenAI
import json
import os
from typing import Dict, Tuple, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def websearch_match(name_a: str, name_b: str, save_evidence: bool = True) -> Tuple[float, Dict]:
    """
    Use OpenAI with web search capability to validate if two organizations are the same.
    
    Args:
        name_a: First organization name
        name_b: Second organization name
        save_evidence: Whether to save search evidence to file
        
    Returns:
        Tuple of (confidence_score, search_details)
    """
    
    # Initialize OpenAI client
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("Warning: OPENAI_API_KEY not set. Returning 0 confidence.")
        return 0.0, {"error": "API key not configured"}
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Use GPT-4o for web search capabilities
        model = os.getenv('TIER3_MODEL', 'gpt-4o')
        
        # System prompt for web research
        system_prompt = """You are an expert healthcare industry researcher with access to current web information. 
Your task is to search the web and determine if two healthcare organization names refer to the same entity.

You should research and consider:
1. Official websites and their "About Us" pages
2. Recent news about mergers, acquisitions, or rebranding
3. Physical addresses and locations
4. Parent company or health system affiliations
5. Leadership and board members
6. Services offered and specialties
7. Hospital or clinic size and capacity
8. Regulatory filings and certifications
9. Historical name changes
10. Any press releases about name changes or consolidations

Be thorough but efficient in your research. Provide specific evidence from credible sources."""

        # User prompt requesting web search
        user_prompt = f"""Search the web for current information about these two healthcare organizations and determine if they refer to the same entity:

Organization 1: {name_a}
Organization 2: {name_b}

Please conduct a thorough web search to find:
- Their official websites
- Any news about mergers, acquisitions, or name changes
- Their physical addresses and locations  
- Parent company relationships
- Any evidence they are the same or different entities

Based on your web research, provide your analysis in the following JSON format:
{{
    "confidence": <0-100 integer representing confidence they are the same entity>,
    "same_entity": <true if same entity, false otherwise>,
    "evidence": "<summary of key evidence found during web search>",
    "sources": ["<list of key sources/websites consulted>"],
    "addresses_found": {{
        "org1": "<address if found>",
        "org2": "<address if found>"
    }},
    "parent_company": "<parent company if identified>",
    "official_websites": {{
        "org1": "<official website if found>",
        "org2": "<official website if found>"
    }},
    "recent_changes": "<any recent mergers, acquisitions, or name changes>",
    "search_timestamp": "{datetime.now().isoformat()}"
}}"""

        # Make the API call with web search
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,  # Low temperature for factual research
            max_tokens=800,   # More tokens for detailed research results
            response_format={"type": "json_object"}
        )
        
        # Parse the response
        result = json.loads(response.choices[0].message.content)
        
        # Extract confidence score
        confidence = float(result.get('confidence', 0))
        
        # Add metadata
        result['model_used'] = model
        result['tokens_used'] = response.usage.total_tokens if response.usage else 0
        result['search_timestamp'] = datetime.now().isoformat()
        
        # Save evidence to file if requested
        if save_evidence and 'evidence' in result:
            save_search_evidence(name_a, name_b, result)
        
        return confidence, result
        
    except json.JSONDecodeError as e:
        print(f"Error parsing OpenAI response: {e}")
        return 0.0, {"error": f"JSON parsing error: {str(e)}"}
        
    except Exception as e:
        print(f"OpenAI API error in web search: {e}")
        return 0.0, {"error": f"API error: {str(e)}"}


def save_search_evidence(name_a: str, name_b: str, result: Dict):
    """
    Save web search evidence to a log file for audit purposes.
    
    Args:
        name_a: First organization name
        name_b: Second organization name
        result: Search results dictionary
    """
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Append to evidence log
        with open('data/websearch_evidence.jsonl', 'a') as f:
            evidence_entry = {
                'timestamp': datetime.now().isoformat(),
                'name_a': name_a,
                'name_b': name_b,
                'confidence': result.get('confidence'),
                'same_entity': result.get('same_entity'),
                'evidence': result.get('evidence'),
                'sources': result.get('sources', []),
                'official_websites': result.get('official_websites', {}),
                'addresses': result.get('addresses_found', {}),
                'parent_company': result.get('parent_company'),
                'recent_changes': result.get('recent_changes')
            }
            f.write(json.dumps(evidence_entry) + '\n')
            
    except Exception as e:
        print(f"Warning: Could not save search evidence: {e}")


def validate_with_web_search(name_pairs: list) -> list:
    """
    Process multiple name pairs through web search validation.
    
    Args:
        name_pairs: List of tuples (name_a, name_b)
        
    Returns:
        List of validation results for each pair
    """
    results = []
    
    for i, (name_a, name_b) in enumerate(name_pairs, 1):
        print(f"Web searching pair {i}/{len(name_pairs)}: {name_a} vs {name_b}")
        confidence, details = websearch_match(name_a, name_b)
        
        results.append({
            'name_a': name_a,
            'name_b': name_b,
            'confidence': confidence,
            'same_entity': details.get('same_entity'),
            'evidence': details.get('evidence'),
            'sources': details.get('sources', []),
            'error': details.get('error')
        })
    
    return results


if __name__ == "__main__":
    # Test the web search matching
    test_pairs = [
        ("Cleveland Clinic", "Cleveland Clinic Foundation"),
        ("Mass General Hospital", "Massachusetts General Hospital"),
        ("NYU Langone", "NYU Medical Center"),
        ("UCSF Medical Center", "University of California San Francisco Hospital"),
        ("Cedars-Sinai", "Cedars-Sinai Medical Center")
    ]
    
    print("Testing Tier 3 Web Search Validation:")
    print("-" * 60)
    print("Note: This requires OPENAI_API_KEY to be set in .env file")
    print("This will use GPT-4o and may take longer due to web searching")
    print("-" * 60)
    
    for name_a, name_b in test_pairs:
        print(f"\nSearching web for: '{name_a}' vs '{name_b}'")
        confidence, details = websearch_match(name_a, name_b)
        
        if 'error' in details:
            print(f"  Error: {details['error']}")
        else:
            print(f"  Confidence: {confidence:.1f}%")
            print(f"  Same Entity: {details.get('same_entity', 'Unknown')}")
            print(f"  Evidence: {details.get('evidence', 'No evidence found')[:200]}...")
            
            if details.get('official_websites'):
                websites = details['official_websites']
                if websites.get('org1'):
                    print(f"  Website 1: {websites['org1']}")
                if websites.get('org2'):
                    print(f"  Website 2: {websites['org2']}")
            
            if details.get('sources'):
                print(f"  Sources consulted: {len(details['sources'])} sources")