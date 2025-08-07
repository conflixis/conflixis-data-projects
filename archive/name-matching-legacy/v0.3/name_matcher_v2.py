"""
Enhanced Name Matching Module V2
Provides fuzzy name matching with configurable thresholds, top-N matches, and confidence levels.
"""

import os
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass

import numpy as np
import pandas as pd
import jellyfish
from rapidfuzz import fuzz


# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Data class for storing match results with confidence levels."""
    name_a_original: str
    name_a_processed: str
    name_b_original: Optional[str]
    name_b_processed: Optional[str]
    scores: Dict[str, float]
    composite_score: float
    confidence_level: str
    rank: int


class EnhancedNameMatcher:
    """Enhanced name matching engine with threshold support and top-N matches."""
    
    DEFAULT_STOPWORDS = set(['stopword'])
    
    def __init__(self, 
                 stopwords: Optional[Set[str]] = None,
                 scoring_weights: Optional[Dict[str, float]] = None,
                 min_score_threshold: float = 60.0,
                 confidence_thresholds: Optional[Dict[str, float]] = None,
                 return_top_n: int = 1):
        """
        Initialize the enhanced name matcher.
        
        Args:
            stopwords: Set of words to remove during preprocessing
            scoring_weights: Dictionary of weights for different scoring methods
            min_score_threshold: Minimum score to consider a match valid
            confidence_thresholds: Dictionary defining confidence levels
            return_top_n: Number of top matches to return for each name
        """
        self.stopwords = stopwords or self.DEFAULT_STOPWORDS
        
        # Default scoring weights
        self.scoring_weights = scoring_weights or {
            'rapidfuzz': 0.1,
            'jellyfish': 0.2,
            'thefuzz': 0.1,
            'token_based': 0.1,
            'first_word': 0.4,
            'partial_match': 0.1
        }
        
        self.min_score_threshold = min_score_threshold
        self.return_top_n = return_top_n
        
        # Default confidence thresholds
        self.confidence_thresholds = confidence_thresholds or {
            'high': 80.0,
            'medium': 60.0,
            'low': 40.0
        }
        
        logger.info(f"Initialized EnhancedNameMatcher with threshold={min_score_threshold}, top_n={return_top_n}")
    
    def preprocess(self, name: str) -> str:
        """Preprocess a name: lowercase, remove stopwords, and keep only alphanumeric characters."""
        if not isinstance(name, str):
            return ""
        
        # Common abbreviation replacements
        name = name.lower()
        name = name.replace("corp.", "corporation")
        name = name.replace("inc.", "incorporated")
        name = name.replace("ltd.", "limited")
        name = name.replace("llc", "limited liability company")
        
        # Keep only alphanumeric, spaces, and hyphens
        name = ''.join([char for char in name if char.isalnum() or char.isspace() or char == '-'])
        
        # Remove stopwords
        words = [word for word in name.split() if word not in self.stopwords]
        
        return ' '.join(words)
    
    def token_based_similarity(self, name1: str, name2: str) -> float:
        """Calculate token-based similarity between two names."""
        tokens1, tokens2 = set(name1.split()), set(name2.split())
        if not tokens1 or not tokens2:
            return 0.0
        
        exact_matches = len(tokens1.intersection(tokens2))
        avg_tokens = (len(tokens1) + len(tokens2)) / 2
        return (exact_matches / avg_tokens) * 100 if avg_tokens > 0 else 0
    
    def first_word_similarity(self, name1: str, name2: str) -> float:
        """Return 100 if the first words match, otherwise 0."""
        words1 = name1.split()
        words2 = name2.split()
        
        if not words1 or not words2:
            return 0.0
            
        return 100.0 if words1[0] == words2[0] else 0.0
    
    def calculate_all_scores(self, name1: str, name2: str) -> Dict[str, float]:
        """Calculate all similarity scores between two names."""
        try:
            scores = {
                'rapidfuzz': fuzz.ratio(name1, name2),
                'jellyfish': jellyfish.jaro_winkler_similarity(name1, name2) * 100,
                'thefuzz': fuzz.token_sort_ratio(name1, name2),
                'token_based': self.token_based_similarity(name1, name2),
                'first_word': self.first_word_similarity(name1, name2),
                'partial_match': fuzz.partial_ratio(name1, name2)
            }
        except Exception as e:
            logger.warning(f"Error calculating scores for '{name1}' and '{name2}': {e}")
            scores = {key: 0.0 for key in self.scoring_weights.keys()}
        
        return scores
    
    def calculate_composite_score(self, scores: Dict[str, float]) -> float:
        """Calculate weighted composite score from individual scores."""
        composite = 0.0
        for key, weight in self.scoring_weights.items():
            if key in scores:
                composite += weight * scores[key]
        return composite
    
    def get_confidence_level(self, score: float) -> str:
        """Determine confidence level based on score."""
        if score >= self.confidence_thresholds['high']:
            return 'High'
        elif score >= self.confidence_thresholds['medium']:
            return 'Medium'
        elif score >= self.confidence_thresholds['low']:
            return 'Low'
        else:
            return 'No Match'
    
    def find_top_n_matches(self, name: str, name_original: str, 
                          df_B: pd.DataFrame, column_name_b: str) -> List[MatchResult]:
        """
        Find top N matches for a given name from dataset B.
        
        Returns:
            List of MatchResult objects sorted by score (highest first)
        """
        all_matches = []
        
        # Calculate scores for all potential matches
        for idx, row_b in df_B.iterrows():
            name_b_original = row_b[column_name_b]
            name_b_processed = row_b.get('Name B PP', self.preprocess(name_b_original))
            
            # Calculate all similarity scores
            scores = self.calculate_all_scores(name, name_b_processed)
            composite_score = self.calculate_composite_score(scores)
            
            # Only consider matches above minimum threshold
            if composite_score >= self.min_score_threshold:
                match_result = MatchResult(
                    name_a_original=name_original,
                    name_a_processed=name,
                    name_b_original=name_b_original,
                    name_b_processed=name_b_processed,
                    scores={f"{k}_score": round(v, 1) for k, v in scores.items()},
                    composite_score=round(composite_score, 1),
                    confidence_level=self.get_confidence_level(composite_score),
                    rank=0  # Will be set after sorting
                )
                all_matches.append(match_result)
        
        # Sort by composite score (highest first) and take top N
        all_matches.sort(key=lambda x: x.composite_score, reverse=True)
        top_matches = all_matches[:self.return_top_n]
        
        # Set ranks
        for i, match in enumerate(top_matches):
            match.rank = i + 1
        
        # If no matches found above threshold, return a single "No Match" result
        if not top_matches:
            no_match_result = MatchResult(
                name_a_original=name_original,
                name_a_processed=name,
                name_b_original=None,
                name_b_processed=None,
                scores={f"{k}_score": 0.0 for k in self.scoring_weights.keys()},
                composite_score=0.0,
                confidence_level='No Match',
                rank=1
            )
            return [no_match_result]
        
        return top_matches
    
    def process_chunk(self, chunk: pd.DataFrame, df_B: pd.DataFrame, 
                     column_name_a: str, column_name_b: str) -> List[Dict]:
        """Process a chunk of rows from dataset A against dataset B."""
        results = []
        
        for _, row_a in chunk.iterrows():
            name_a_original = row_a[column_name_a]
            name_a_processed = self.preprocess(name_a_original)
            
            # Find top N matches
            matches = self.find_top_n_matches(
                name_a_processed, name_a_original, df_B, column_name_b
            )
            
            # Convert MatchResult objects to dictionaries for DataFrame
            for match in matches:
                result_dict = {
                    column_name_a: match.name_a_original,
                    "Name A PP": match.name_a_processed,
                    f"{column_name_b}_match_{match.rank}": match.name_b_original,
                    f"Name B PP_match_{match.rank}": match.name_b_processed,
                    f"Composite Score_match_{match.rank}": match.composite_score,
                    f"Confidence_match_{match.rank}": match.confidence_level,
                    "Match Rank": match.rank
                }
                
                # Add individual scores
                for score_name, score_value in match.scores.items():
                    result_dict[f"{score_name}_match_{match.rank}"] = score_value
                
                results.append(result_dict)
        
        return results
    
    def find_matches(self, df_A: pd.DataFrame, df_B: pd.DataFrame,
                    column_name_a: str, column_name_b: str,
                    chunk_size: int = 50, max_workers: int = 10,
                    cache_preprocessed: bool = True) -> pd.DataFrame:
        """
        Find matches using parallel processing with enhanced features.
        
        Returns:
            DataFrame with top N matches for each name from df_A
        """
        # Validate input
        if column_name_a not in df_A.columns:
            raise ValueError(f"Column '{column_name_a}' not found in dataset A")
        if column_name_b not in df_B.columns:
            raise ValueError(f"Column '{column_name_b}' not found in dataset B")
        
        # Cache preprocessed names for df_B if requested
        if cache_preprocessed and 'Name B PP' not in df_B.columns:
            logger.info("Preprocessing names from dataset B...")
            df_B['Name B PP'] = df_B[column_name_b].apply(self.preprocess)
        
        results = []
        total_rows = len(df_A)
        
        # Create chunks for parallel processing
        chunks = []
        for i in range(0, total_rows, chunk_size):
            chunk = df_A.iloc[i:i + chunk_size]
            if not chunk.empty:
                chunks.append(chunk)
        
        logger.info(f"Processing {total_rows} rows in {len(chunks)} chunks")
        
        # Process chunks in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.process_chunk, chunk, df_B, column_name_a, column_name_b): len(chunk)
                for chunk in chunks
            }
            
            total_processed = 0
            for future in as_completed(futures):
                try:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    chunk_size_processed = futures[future]
                    total_processed += chunk_size_processed
                    
                    progress = (total_processed / total_rows) * 100
                    logger.info(f"Progress: {total_processed}/{total_rows} ({progress:.1f}%)")
                    
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
        
        # Create DataFrame from results
        if not results:
            logger.warning("No results found")
            return pd.DataFrame()
        
        results_df = pd.DataFrame(results)
        
        # Log summary statistics
        if self.return_top_n == 1:
            confidence_counts = results_df['Confidence_match_1'].value_counts()
            logger.info(f"Match summary: {confidence_counts.to_dict()}")
        
        return results_df


# Convenience function for backward compatibility
def enhanced_find_matches_v2(df_A: pd.DataFrame, df_B: pd.DataFrame,
                            column_name_a: str, column_name_b: str,
                            config: Optional[Dict] = None) -> pd.DataFrame:
    """
    Find matches using the EnhancedNameMatcher with configuration.
    
    Args:
        df_A: First dataframe
        df_B: Second dataframe
        column_name_a: Column name in df_A
        column_name_b: Column name in df_B
        config: Configuration dictionary
        
    Returns:
        DataFrame with match results
    """
    if config is None:
        config = {}
    
    # Extract configuration
    matching_config = config.get('matching', {})
    processing_config = config.get('processing', {})
    scoring_weights = config.get('scoring_weights', None)
    
    # Initialize matcher
    matcher = EnhancedNameMatcher(
        stopwords=set(config.get('stopwords', ['stopword'])),
        scoring_weights=scoring_weights,
        min_score_threshold=matching_config.get('min_score_threshold', 60.0),
        confidence_thresholds=matching_config.get('confidence_thresholds'),
        return_top_n=matching_config.get('return_top_n_matches', 1)
    )
    
    # Perform matching
    return matcher.find_matches(
        df_A, df_B,
        column_name_a, column_name_b,
        chunk_size=processing_config.get('chunk_size', 50),
        max_workers=processing_config.get('max_workers', 10)
    )