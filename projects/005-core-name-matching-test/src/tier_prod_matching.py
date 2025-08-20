"""
Tier-prod: Full PR1362 Production Matching Approach
Implements Elasticsearch-style multi-strategy search with AI enhancement
Based on conflixis-engine PR #1362 company-matching-api
"""

import os
import time
from typing import Dict, Tuple, List, Optional
from rapidfuzz import fuzz
import jellyfish
import re
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Thresholds from PR1362
FAST_PATH_THRESHOLD = 95.0  # Skip AI above this
HIGH_CONFIDENCE_THRESHOLD = 90.0  # Single match threshold
MEDIUM_CONFIDENCE_THRESHOLD = 30.0  # Multiple matches threshold
NO_MATCH_THRESHOLD = 30.0  # Below this = no match

# Boost scores from PR1362 Elasticsearch config
BOOST_EXACT = 10.0
BOOST_PHRASE = 7.0
BOOST_FUZZY = 5.0
BOOST_FIRST_WORD = 4.0
BOOST_NORMALIZED = 3.0
BOOST_WILDCARD = 2.0


def get_auto_fuzziness(text: str) -> int:
    """
    Elasticsearch AUTO fuzziness logic.
    Returns allowed edit distance based on text length.
    """
    length = len(text)
    if length <= 2:
        return 0  # Exact match only
    elif length <= 5:
        return 1  # 1 edit allowed
    else:
        return 2  # 2 edits allowed


def calculate_edit_distance(str1: str, str2: str) -> int:
    """Calculate Levenshtein edit distance between two strings."""
    if len(str1) < len(str2):
        return calculate_edit_distance(str2, str1)
    
    if len(str2) == 0:
        return len(str1)
    
    previous_row = range(len(str2) + 1)
    for i, c1 in enumerate(str1):
        current_row = [i + 1]
        for j, c2 in enumerate(str2):
            # j+1 instead of j since previous_row and current_row are one character longer than str2
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def preprocess_name(name: str) -> str:
    """
    Preprocess company name for matching (normalization).
    Based on PR1362's normalizeCompanyName function.
    """
    if not name:
        return ""
    
    name = str(name).lower().strip()
    
    # Remove common suffixes
    suffixes = [
        r'\binc\.?$', r'\bincorporated$', r'\bcorp\.?$', r'\bcorporation$',
        r'\bllc\.?$', r'\bllp\.?$', r'\blp\.?$', r'\bltd\.?$', r'\blimited$',
        r'\bco\.?$', r'\bcompany$', r'\bplc\.?$', r'\bgmbh$', r'\bsa$', r'\bag$'
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Healthcare-specific expansions (our addition)
    replacements = {
        r'\bhosp\.?\b': 'hospital',
        r'\bmed\.?\b': 'medical',
        r'\bctr\.?\b': 'center',
        r'\bhc\b': 'healthcare',
        r'\bhlth\b': 'health',
        r'\bsvcs?\b': 'services',
        r'\bmgmt\b': 'management',
        r'\bgrp\.?\b': 'group',
        r'\buniv\.?\b': 'university',
        r'\bsys\.?\b': 'system',
    }
    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name)
    
    # Remove special characters but keep spaces
    name = re.sub(r'[^\w\s]', ' ', name)
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name


def elasticsearch_style_search(query: str, candidate: str) -> Dict[str, float]:
    """
    Mimics Elasticsearch's multi-strategy search with boost scores.
    Returns individual strategy scores.
    """
    query_normalized = preprocess_name(query)
    candidate_normalized = preprocess_name(candidate)
    
    scores = {}
    
    # 1. Exact match (boost: 10)
    if query.lower() == candidate.lower():
        scores['exact'] = 100.0 * BOOST_EXACT
    else:
        scores['exact'] = 0.0
    
    # 2. Phrase match (boost: 7) - token order matters
    # Using token_sort_ratio as proxy for phrase matching
    phrase_score = fuzz.token_sort_ratio(query.lower(), candidate.lower())
    scores['phrase'] = phrase_score * BOOST_PHRASE / 100.0
    
    # 3. Fuzzy match with AUTO fuzziness (boost: 5)
    edit_dist = calculate_edit_distance(query.lower(), candidate.lower())
    max_allowed = get_auto_fuzziness(query)
    if edit_dist <= max_allowed:
        # Convert edit distance to similarity score
        fuzzy_score = max(0, 100 * (1 - edit_dist / max(len(query), len(candidate))))
        scores['fuzzy'] = fuzzy_score * BOOST_FUZZY / 100.0
    else:
        scores['fuzzy'] = fuzz.ratio(query.lower(), candidate.lower()) * BOOST_FUZZY / 100.0
    
    # 4. First word match (boost: 4) - healthcare addition
    query_words = query_normalized.split()
    candidate_words = candidate_normalized.split()
    if query_words and candidate_words and query_words[0] == candidate_words[0]:
        scores['first_word'] = 100.0 * BOOST_FIRST_WORD / 100.0
    else:
        scores['first_word'] = 0.0
    
    # 5. Normalized fuzzy (boost: 3)
    normalized_score = fuzz.ratio(query_normalized, candidate_normalized)
    scores['normalized'] = normalized_score * BOOST_NORMALIZED / 100.0
    
    # 6. Wildcard/substring match (boost: 2)
    if query_normalized in candidate_normalized or candidate_normalized in query_normalized:
        scores['wildcard'] = 100.0 * BOOST_WILDCARD / 100.0
    else:
        scores['wildcard'] = 0.0
    
    return scores


def calculate_confidence(query: str, candidate: str, es_scores: Dict[str, float]) -> float:
    """
    Calculate overall confidence score combining multiple signals.
    Based on PR1362's confidence calculation logic.
    """
    # Get the maximum ES-style score (mimicking Elasticsearch's relevance score)
    max_es_score = max(es_scores.values()) if es_scores else 0.0
    
    # Additional string similarity metrics
    query_norm = preprocess_name(query)
    candidate_norm = preprocess_name(candidate)
    
    # Multiple similarity algorithms (from PR1362)
    string_similarities = {
        'ratio': fuzz.ratio(query_norm, candidate_norm) / 100.0,
        'partial': fuzz.partial_ratio(query_norm, candidate_norm) / 100.0,
        'token_set': fuzz.token_set_ratio(query_norm, candidate_norm) / 100.0,
        'jaro_winkler': jellyfish.jaro_winkler_similarity(query_norm, candidate_norm)
    }
    
    # Check for abbreviation
    is_abbreviation = is_potential_abbreviation(query, candidate)
    
    # Combine signals (simplified version of PR1362's calculateFinalConfidence)
    # Normalize ES score to 0-1 range
    normalized_es = min(max_es_score / (BOOST_EXACT * 100), 1.0)
    
    # Weight the different signals
    weights = {
        'es_score': 0.4,
        'string_sim': 0.3,
        'abbreviation': 0.2,
        'length_ratio': 0.1
    }
    
    # Calculate weighted score
    avg_string_sim = sum(string_similarities.values()) / len(string_similarities)
    length_ratio = min(len(query_norm), len(candidate_norm)) / max(len(query_norm), len(candidate_norm))
    
    confidence = (
        normalized_es * weights['es_score'] +
        avg_string_sim * weights['string_sim'] +
        (1.0 if is_abbreviation else 0.5) * weights['abbreviation'] +
        length_ratio * weights['length_ratio']
    )
    
    # Convert to percentage
    return min(confidence * 100, 100.0)


def is_potential_abbreviation(text1: str, text2: str) -> bool:
    """
    Check if one text might be an abbreviation of the other.
    Based on PR1362's isPotentialAbbreviation.
    """
    t1_clean = preprocess_name(text1)
    t2_clean = preprocess_name(text2)
    
    # Check if one is much shorter (potential abbreviation)
    if len(t1_clean) < 5 and len(t2_clean) > 10:
        # Check if short one's letters appear in order in long one
        t1_letters = [c for c in t1_clean if c.isalpha()]
        t2_words = t2_clean.split()
        t2_initials = [w[0] for w in t2_words if w]
        
        if t1_letters == t2_initials:
            return True
    
    # Check reverse
    if len(t2_clean) < 5 and len(t1_clean) > 10:
        t2_letters = [c for c in t2_clean if c.isalpha()]
        t1_words = t1_clean.split()
        t1_initials = [w[0] for w in t1_words if w]
        
        if t2_letters == t1_initials:
            return True
    
    return False


def enhance_with_ai(query: str, candidate: str, initial_confidence: float, model: str = None) -> Tuple[float, Dict]:
    """
    Use OpenAI to enhance medium-confidence matches.
    Based on PR1362's AIEnhancementService.
    """
    if not model:
        model = os.getenv('TIER_PROD_MODEL', 'gpt-4o-mini')
    
    client = OpenAI()
    
    # PR1362-style prompt
    prompt = f"""You are an expert at determining if two organization names refer to the same entity.

Analyze these two names:
Name A: "{query}"
Name B: "{candidate}"

Initial confidence: {initial_confidence:.1f}%

Consider:
1. Are these the same organization?
2. Could one be an abbreviation, subsidiary, or alternate name of the other?
3. Are there typos or minor variations?
4. Do they represent the same legal entity?

Provide your assessment as:
- confidence: 0-100 (your confidence they match)
- reasoning: Brief explanation
- match_type: exact|abbreviation|subsidiary|variation|different

Respond in JSON format:
{{"confidence": <number>, "reasoning": "<text>", "match_type": "<type>"}}"""

    try:
        start_time = time.time()
        
        # Handle different model types
        if 'gpt-5' in model:
            # Use the new gpt-5 API structure
            response = client.responses.create(
                model=model,
                input=[{"role": "user", "content": prompt}],
                text={"format": {"type": "json_object"}, "verbosity": "low"},
                reasoning={"effort": "high"}
            )
            
            # Parse gpt-5 response
            result_text = response.output_text
        else:
            # Standard chat completion for gpt-4/gpt-3.5
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert at company name matching. Always respond in valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            result_text = response.choices[0].message.content
        
        elapsed = time.time() - start_time
        
        # Parse response
        import json
        result = json.loads(result_text)
        
        # Combine AI confidence with initial confidence
        # PR1362 uses weighted combination
        final_confidence = (initial_confidence * 0.4 + result['confidence'] * 0.6)
        
        return final_confidence, {
            'ai_confidence': result['confidence'],
            'initial_confidence': initial_confidence,
            'final_confidence': final_confidence,
            'reasoning': result.get('reasoning', ''),
            'match_type': result.get('match_type', 'unknown'),
            'model_used': model,
            'processing_time': elapsed
        }
        
    except Exception as e:
        print(f"AI enhancement error: {e}")
        # Fall back to initial confidence
        return initial_confidence, {
            'error': str(e),
            'initial_confidence': initial_confidence,
            'final_confidence': initial_confidence
        }


def tier_prod_match(name_a: str, name_b: str, model: str = None) -> Tuple[float, Dict]:
    """
    Main Tier-prod matching function implementing full PR1362 approach.
    
    Returns:
        Tuple of (confidence_score, details_dict)
    """
    start_time = time.time()
    
    # Phase 1: Elasticsearch-style search
    es_scores = elasticsearch_style_search(name_a, name_b)
    
    # Phase 2: Calculate initial confidence
    initial_confidence = calculate_confidence(name_a, name_b, es_scores)
    
    # Phase 3: Determine path based on confidence
    details = {
        'es_scores': es_scores,
        'initial_confidence': initial_confidence,
        'tier_reached': 'initial',
        'processing_time': 0
    }
    
    # Fast path: Very high confidence (>95%)
    if initial_confidence >= FAST_PATH_THRESHOLD:
        details['tier_reached'] = 'fast_path'
        details['final_confidence'] = initial_confidence
        details['skip_reason'] = 'high_confidence'
        details['processing_time'] = time.time() - start_time
        return initial_confidence, details
    
    # No match: Very low confidence (<30%)
    if initial_confidence < NO_MATCH_THRESHOLD:
        details['tier_reached'] = 'no_match'
        details['final_confidence'] = initial_confidence
        details['skip_reason'] = 'low_confidence'
        details['processing_time'] = time.time() - start_time
        return initial_confidence, details
    
    # Phase 4: AI Enhancement for medium confidence (30-95%)
    details['tier_reached'] = 'ai_enhanced'
    enhanced_confidence, ai_details = enhance_with_ai(name_a, name_b, initial_confidence, model)
    
    details.update(ai_details)
    details['final_confidence'] = enhanced_confidence
    details['processing_time'] = time.time() - start_time
    
    return enhanced_confidence, details


if __name__ == "__main__":
    # Test the implementation
    test_pairs = [
        ("Microsoft Corporation", "Microsoft Corp"),  # Should be high confidence
        ("Apple Inc", "Samsung Electronics"),  # Should be no match
        ("Johns Hopkins Hospital", "Johns Hopkins Medical Center"),  # Medium, needs AI
    ]
    
    for name_a, name_b in test_pairs:
        confidence, details = tier_prod_match(name_a, name_b)
        print(f"\n{name_a} vs {name_b}")
        print(f"  Confidence: {confidence:.2f}%")
        print(f"  Tier: {details['tier_reached']}")
        if 'ai_confidence' in details:
            print(f"  AI confidence: {details['ai_confidence']:.2f}%")