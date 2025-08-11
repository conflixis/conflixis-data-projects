"""
Tier 1: Enhanced Fuzzy Matching for Healthcare Organizations
Uses multiple algorithms to calculate similarity scores
"""

from rapidfuzz import fuzz
import jellyfish
import re
from typing import Dict, Tuple


def fuzzy_match(name_a: str, name_b: str) -> Tuple[float, Dict[str, float]]:
    """
    Perform healthcare-optimized fuzzy matching using multiple algorithms.
    
    Args:
        name_a: First organization name
        name_b: Second organization name
        
    Returns:
        Tuple of (final_score, individual_scores_dict)
    """
    
    # Preprocess both names
    clean_a = preprocess_healthcare(name_a)
    clean_b = preprocess_healthcare(name_b)
    
    # Calculate various similarity scores
    scores = {
        'exact': 100.0 if clean_a == clean_b else 0.0,
        'ratio': fuzz.ratio(clean_a, clean_b),
        'partial': fuzz.partial_ratio(clean_a, clean_b),
        'token_sort': fuzz.token_sort_ratio(clean_a, clean_b),
        'token_set': fuzz.token_set_ratio(clean_a, clean_b),
        'jaro_winkler': jellyfish.jaro_winkler_similarity(clean_a, clean_b) * 100
    }
    
    # Check for first word match (important for organizations)
    words_a = clean_a.split()
    words_b = clean_b.split()
    if words_a and words_b:
        scores['first_word'] = 100.0 if words_a[0] == words_b[0] else 0.0
    else:
        scores['first_word'] = 0.0
    
    # Calculate weighted average
    weights = {
        'exact': 0.25,
        'ratio': 0.15,
        'partial': 0.10,
        'token_sort': 0.15,
        'token_set': 0.10,
        'jaro_winkler': 0.15,
        'first_word': 0.10
    }
    
    final_score = sum(scores[k] * weights[k] for k in scores)
    
    return final_score, scores


def preprocess_healthcare(name: str) -> str:
    """
    Preprocess healthcare organization names for better matching.
    
    Args:
        name: Organization name to preprocess
        
    Returns:
        Cleaned and standardized name
    """
    if not name:
        return ""
    
    name = str(name).lower().strip()
    
    # Healthcare-specific abbreviation expansions
    replacements = {
        r'\bhosp\.?\b': 'hospital',
        r'\bhosps\.?\b': 'hospitals',
        r'\bmed\.?\b': 'medical',
        r'\bctr\.?\b': 'center',
        r'\bctrs\.?\b': 'centers',
        r'\bhc\b': 'healthcare',
        r'\bhca\b': 'healthcare',
        r'\bhlth\b': 'health',
        r'\bsvcs?\b': 'services',
        r'\bmgmt\b': 'management',
        r'\bassoc\.?\b': 'associates',
        r'\bgrp\.?\b': 'group',
        r'\buniv\.?\b': 'university',
        r'\bsys\.?\b': 'system',
        r'\bphys\.?\b': 'physicians',
        r'\bclin\.?\b': 'clinic',
        r'\brehab\.?\b': 'rehabilitation',
        r'\bspec\.?\b': 'specialty',
        r'\bemerg\.?\b': 'emergency',
        r'\bsurg\.?\b': 'surgical',
        r'\bortho\.?\b': 'orthopedic',
        r'\bcardio\.?\b': 'cardiovascular',
        r'\bpeds?\b': 'pediatric',
        r'\bob\/gyn\b': 'obstetrics gynecology',
        r'\bobgyn\b': 'obstetrics gynecology',
        r'\ber\b': 'emergency room',
        r'\bicu\b': 'intensive care unit',
        r'\bdba\b': '',  # Remove "doing business as"
        r'\bfka\b': '',  # Remove "formerly known as"
        r'\bcorp\.?\b': 'corporation',
        r'\binc\.?\b': 'incorporated',
        r'\bllc\b': 'limited liability company',
        r'\bllp\b': 'limited liability partnership',
        r'\bltd\.?\b': 'limited',
        r'\bco\.?\b': 'company',
        r'\bst\.?\b': 'saint',
        r'\bmt\.?\b': 'mount'
    }
    
    # Apply replacements
    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name)
    
    # Remove common suffixes that don't add value
    suffixes_to_remove = [
        'incorporated', 'corporation', 'limited liability company', 
        'limited liability partnership', 'limited', 'company',
        'llc', 'llp', 'inc', 'corp', 'ltd', 'co'
    ]
    
    for suffix in suffixes_to_remove:
        if name.endswith(f' {suffix}'):
            name = name[:-len(suffix)-1].strip()
    
    # Remove punctuation except hyphens and spaces
    name = re.sub(r'[^\w\s-]', '', name)
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name


def calculate_confidence_level(score: float) -> str:
    """
    Determine confidence level based on score.
    
    Args:
        score: Matching score (0-100)
        
    Returns:
        Confidence level string
    """
    if score >= 90:
        return "HIGH"
    elif score >= 75:
        return "MEDIUM"
    elif score >= 60:
        return "LOW"
    else:
        return "VERY_LOW"


if __name__ == "__main__":
    # Test the fuzzy matching
    test_pairs = [
        ("St. Mary's Hospital", "Saint Marys Medical Center"),
        ("ABC Healthcare LLC", "ABC Health System"),
        ("Regional Med. Ctr.", "Regional Medical Center"),
        ("University Hosp.", "University Hospital System"),
        ("Community Hospital Inc.", "Community Hosp")
    ]
    
    print("Testing Tier 1 Fuzzy Matching:")
    print("-" * 60)
    
    for name_a, name_b in test_pairs:
        score, details = fuzzy_match(name_a, name_b)
        confidence = calculate_confidence_level(score)
        
        print(f"\nName A: {name_a}")
        print(f"Name B: {name_b}")
        print(f"Final Score: {score:.1f} ({confidence})")
        print("Individual Scores:")
        for key, value in details.items():
            print(f"  {key}: {value:.1f}")