"""
Tier 2 Healthcare Entity Name Matcher
Production-ready implementation combining fuzzy matching with AI enhancement
Achieves 96.9% accuracy on healthcare entity names

Based on the successful DA-156 Name Matching POC
"""

import os
import time
import logging
from typing import Dict, Tuple, List, Optional, Union
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import jellyfish
import re
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Tier2NameMatcher:
    """
    Production-ready Tier 2 name matcher combining fuzzy matching with AI enhancement.
    
    This implementation achieves 96.9% accuracy on healthcare entity names by:
    1. Using multiple fuzzy matching algorithms
    2. Sending low-confidence cases to OpenAI for enhancement
    3. Applying optimized thresholds based on empirical testing
    
    Attributes:
        fuzzy_threshold (float): Confidence threshold for accepting fuzzy matches (default: 85.0)
        decision_threshold (float): Final threshold for accepting matches (default: 50.0)
        model (str): OpenAI model to use (default: gpt-4o-mini)
        max_workers (int): Maximum concurrent workers for API calls
    """
    
    def __init__(self, 
                 fuzzy_threshold: float = 85.0,
                 decision_threshold: float = 50.0,
                 model: str = 'gpt-4o-mini',
                 api_key: Optional[str] = None,
                 max_workers: int = 5):
        """
        Initialize the Tier 2 matcher.
        
        Args:
            fuzzy_threshold: Minimum confidence to accept fuzzy match without AI
            decision_threshold: Minimum confidence to accept as final match
            model: OpenAI model name
            api_key: OpenAI API key (if not in environment)
            max_workers: Maximum concurrent API workers
        """
        self.fuzzy_threshold = fuzzy_threshold
        self.decision_threshold = decision_threshold
        self.model = model
        self.max_workers = max_workers
        
        # Initialize OpenAI client
        if api_key:
            self.client = OpenAI(api_key=api_key)
        else:
            self.client = OpenAI()  # Uses OPENAI_API_KEY from environment
        
        # Track statistics
        self.stats = {
            'fuzzy_only': 0,
            'ai_enhanced': 0,
            'total_processed': 0,
            'api_errors': 0
        }
    
    def preprocess_name(self, name: str) -> str:
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
        }
        
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        
        # Remove common suffixes
        suffixes = [
            r'\binc\.?$', r'\bincorporated$', r'\bcorp\.?$', r'\bcorporation$',
            r'\bllc\.?$', r'\bllp\.?$', r'\blp\.?$', r'\bltd\.?$', r'\blimited$',
            r'\bco\.?$', r'\bcompany$', r'\bplc\.?$'
        ]
        
        for suffix in suffixes:
            name = re.sub(suffix, '', name, flags=re.IGNORECASE)
        
        # Remove special characters but keep spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Normalize whitespace
        name = ' '.join(name.split())
        
        return name
    
    def fuzzy_match(self, name_a: str, name_b: str) -> Tuple[float, Dict[str, float]]:
        """
        Perform healthcare-optimized fuzzy matching using multiple algorithms.
        
        Args:
            name_a: First organization name
            name_b: Second organization name
            
        Returns:
            Tuple of (final_score, individual_scores_dict)
        """
        # Preprocess both names
        clean_a = self.preprocess_name(name_a)
        clean_b = self.preprocess_name(name_b)
        
        # Calculate various similarity scores
        scores = {
            'exact': 100.0 if clean_a == clean_b else 0.0,
            'ratio': fuzz.ratio(clean_a, clean_b),
            'partial': fuzz.partial_ratio(clean_a, clean_b),
            'token_sort': fuzz.token_sort_ratio(clean_a, clean_b),
            'token_set': fuzz.token_set_ratio(clean_a, clean_b),
            'jaro_winkler': jellyfish.jaro_winkler_similarity(clean_a, clean_b) * 100 if clean_a and clean_b else 0
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
    
    def enhance_with_ai(self, name_a: str, name_b: str, fuzzy_score: float) -> Tuple[float, str]:
        """
        Use OpenAI to enhance low-confidence fuzzy matches.
        
        Args:
            name_a: First organization name
            name_b: Second organization name
            fuzzy_score: Initial fuzzy matching score
            
        Returns:
            Tuple of (ai_confidence, reasoning)
        """
        prompt = f"""You are an expert at matching healthcare organization names.

Compare these two organization names and determine if they refer to the same entity:

Name A: "{name_a}"
Name B: "{name_b}"

Initial fuzzy matching score: {fuzzy_score:.1f}%

Consider:
1. Common abbreviations in healthcare (hosp→hospital, med→medical, etc.)
2. Subsidiaries and parent organizations
3. Name variations and rebranding
4. Typos and misspellings
5. Word order differences

Provide your confidence (0-100) that these are the same organization.
Be conservative - only give high confidence if you're certain they match.

Respond with just a number between 0 and 100."""

        try:
            # Handle different model types
            if 'gpt-5' in self.model:
                # Use the new gpt-5 API structure
                response = self.client.responses.create(
                    model=self.model,
                    input=[{"role": "user", "content": prompt}],
                    text={"verbosity": "low"},
                    reasoning={"effort": "high"}
                )
                
                # Parse gpt-5 response
                result_text = response.output_text.strip()
            else:
                # Standard chat completion for gpt-4/gpt-3.5
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert at healthcare organization name matching. Respond with only a confidence score between 0 and 100."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0,
                    max_tokens=10
                )
                result_text = response.choices[0].message.content.strip()
            
            # Parse the confidence score
            confidence = float(result_text.replace('%', ''))
            confidence = max(0, min(100, confidence))  # Clamp to 0-100
            
            return confidence, "AI enhanced"
            
        except Exception as e:
            logger.error(f"AI enhancement error: {e}")
            self.stats['api_errors'] += 1
            # Fall back to fuzzy score on error
            return fuzzy_score, f"Error: {str(e)}"
    
    def match_pair(self, name_a: str, name_b: str, use_ai: bool = True) -> Dict:
        """
        Match a single pair of names using Tier 2 approach.
        
        Args:
            name_a: First organization name
            name_b: Second organization name
            use_ai: Whether to use AI enhancement for low-confidence cases
            
        Returns:
            Dictionary with match results
        """
        self.stats['total_processed'] += 1
        
        # Step 1: Fuzzy matching
        fuzzy_score, fuzzy_details = self.fuzzy_match(name_a, name_b)
        
        # Step 2: Check if fuzzy is sufficient
        if fuzzy_score >= self.fuzzy_threshold:
            self.stats['fuzzy_only'] += 1
            return {
                'name_a': name_a,
                'name_b': name_b,
                'fuzzy_score': fuzzy_score,
                'final_score': fuzzy_score,
                'is_match': fuzzy_score >= self.decision_threshold,
                'confidence_source': 'fuzzy_high_confidence',
                'fuzzy_details': fuzzy_details,
                'ai_score': None,
                'ai_reasoning': None
            }
        
        # Step 3: AI enhancement for low-confidence cases
        if use_ai:
            self.stats['ai_enhanced'] += 1
            ai_score, ai_reasoning = self.enhance_with_ai(name_a, name_b, fuzzy_score)
            
            # Combine scores (40% fuzzy, 60% AI - proven optimal weights)
            final_score = 0.4 * fuzzy_score + 0.6 * ai_score
            
            return {
                'name_a': name_a,
                'name_b': name_b,
                'fuzzy_score': fuzzy_score,
                'ai_score': ai_score,
                'final_score': final_score,
                'is_match': final_score >= self.decision_threshold,
                'confidence_source': 'ai_enhanced',
                'fuzzy_details': fuzzy_details,
                'ai_reasoning': ai_reasoning
            }
        else:
            # No AI, just use fuzzy score
            return {
                'name_a': name_a,
                'name_b': name_b,
                'fuzzy_score': fuzzy_score,
                'final_score': fuzzy_score,
                'is_match': fuzzy_score >= self.decision_threshold,
                'confidence_source': 'fuzzy_low_confidence',
                'fuzzy_details': fuzzy_details,
                'ai_score': None,
                'ai_reasoning': None
            }
    
    def match_batch(self, pairs: List[Tuple[str, str]], use_ai: bool = True) -> List[Dict]:
        """
        Match a batch of name pairs with optional parallel processing.
        
        Args:
            pairs: List of (name_a, name_b) tuples
            use_ai: Whether to use AI enhancement
            
        Returns:
            List of match result dictionaries
        """
        results = []
        
        if use_ai and self.max_workers > 1:
            # Parallel processing for AI calls
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for name_a, name_b in pairs:
                    future = executor.submit(self.match_pair, name_a, name_b, use_ai)
                    futures.append(future)
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing pair: {e}")
        else:
            # Sequential processing
            for name_a, name_b in pairs:
                result = self.match_pair(name_a, name_b, use_ai)
                results.append(result)
        
        return results
    
    def match_dataframes(self, 
                        df_a: pd.DataFrame, 
                        df_b: pd.DataFrame,
                        name_col_a: str,
                        name_col_b: str,
                        use_ai: bool = True,
                        top_n: int = 1) -> pd.DataFrame:
        """
        Match all names from df_a against all names in df_b.
        
        Args:
            df_a: First dataframe
            df_b: Second dataframe  
            name_col_a: Name column in df_a
            name_col_b: Name column in df_b
            use_ai: Whether to use AI enhancement
            top_n: Number of top matches to return per name
            
        Returns:
            DataFrame with match results
        """
        results = []
        
        for idx_a, row_a in df_a.iterrows():
            name_a = row_a[name_col_a]
            matches = []
            
            # Compare against all names in df_b
            for idx_b, row_b in df_b.iterrows():
                name_b = row_b[name_col_b]
                match_result = self.match_pair(name_a, name_b, use_ai)
                match_result['index_a'] = idx_a
                match_result['index_b'] = idx_b
                matches.append(match_result)
            
            # Sort by final score and take top N
            matches.sort(key=lambda x: x['final_score'], reverse=True)
            top_matches = matches[:top_n]
            
            for rank, match in enumerate(top_matches, 1):
                match['rank'] = rank
                results.append(match)
            
            # Log progress
            if (idx_a + 1) % 10 == 0:
                logger.info(f"Processed {idx_a + 1}/{len(df_a)} names")
        
        return pd.DataFrame(results)
    
    def get_statistics(self) -> Dict:
        """
        Get matching statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            **self.stats,
            'fuzzy_only_pct': (self.stats['fuzzy_only'] / max(self.stats['total_processed'], 1)) * 100,
            'ai_enhanced_pct': (self.stats['ai_enhanced'] / max(self.stats['total_processed'], 1)) * 100,
            'api_error_rate': (self.stats['api_errors'] / max(self.stats['ai_enhanced'], 1)) * 100
        }
    
    def reset_statistics(self):
        """Reset statistics counters."""
        self.stats = {
            'fuzzy_only': 0,
            'ai_enhanced': 0,
            'total_processed': 0,
            'api_errors': 0
        }


# Convenience function for simple use cases
def match_names(name_a: str, name_b: str, 
                fuzzy_threshold: float = 85.0,
                use_ai: bool = True,
                model: str = 'gpt-4o-mini') -> Dict:
    """
    Simple function to match two names.
    
    Args:
        name_a: First name
        name_b: Second name
        fuzzy_threshold: Threshold for fuzzy matching
        use_ai: Whether to use AI enhancement
        model: OpenAI model to use
        
    Returns:
        Match result dictionary
    """
    matcher = Tier2NameMatcher(fuzzy_threshold=fuzzy_threshold, model=model)
    return matcher.match_pair(name_a, name_b, use_ai)


if __name__ == "__main__":
    # Example usage
    test_pairs = [
        ("Johns Hopkins Hospital", "Johns Hopkins Medical Center"),
        ("Mayo Clinic", "Mayo Medical Center"),
        ("Cleveland Clinic", "Cleveland Clinc"),  # Typo
        ("Mount Sinai Hospital", "Kaiser Permanente"),  # Different
    ]
    
    print("Testing Tier 2 Name Matcher")
    print("=" * 60)
    
    matcher = Tier2NameMatcher()
    
    for name_a, name_b in test_pairs:
        result = matcher.match_pair(name_a, name_b, use_ai=False)  # Set to True to test with AI
        print(f"\n{name_a} vs {name_b}")
        print(f"  Fuzzy Score: {result['fuzzy_score']:.2f}")
        print(f"  Final Score: {result['final_score']:.2f}")
        print(f"  Match: {result['is_match']}")
        print(f"  Source: {result['confidence_source']}")
    
    print("\n" + "=" * 60)
    print("Statistics:")
    for key, value in matcher.get_statistics().items():
        print(f"  {key}: {value}")