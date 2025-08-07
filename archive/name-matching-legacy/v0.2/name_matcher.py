"""
Core Name Matching Module
Provides functions for fuzzy name matching between two datasets.
"""

import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import jellyfish
from rapidfuzz import fuzz


# Default stopwords for preprocessing
DEFAULT_STOPWORDS = set(['stopword'])


class NameMatcher:
    """Name matching engine with configurable parameters."""
    
    def __init__(self, stopwords: Optional[Set[str]] = None, 
                 scoring_weights: Optional[Dict[str, float]] = None):
        """
        Initialize the name matcher.
        
        Args:
            stopwords: Set of words to remove during preprocessing
            scoring_weights: Dictionary of weights for different scoring methods
        """
        self.stopwords = stopwords or DEFAULT_STOPWORDS
        
        # Default scoring weights
        self.scoring_weights = scoring_weights or {
            'rapidfuzz': 0.1,
            'jellyfish': 0.2,
            'thefuzz': 0.1,
            'token_based': 0.1,
            'first_word': 0.4,
            'partial_match': 0.1
        }
    
    def preprocess(self, name: str) -> str:
        """Preprocess a name: lowercase, remove stopwords, and keep only alphanumeric characters."""
        if not isinstance(name, str):
            return ""
        name = name.lower().replace("corp.", "corporation")
        name = ''.join([char for char in name if char.isalnum() or char.isspace() or char == '-'])
        return ' '.join([word for word in name.split() if word not in self.stopwords])
    
    def token_based_similarity(self, name1: str, name2: str) -> float:
        """Calculate token-based similarity between two names."""
        tokens1, tokens2 = set(name1.split()), set(name2.split())
        exact_matches = len(tokens1.intersection(tokens2))
        avg_tokens = (len(tokens1) + len(tokens2)) / 2
        return (exact_matches / avg_tokens) * 100 if avg_tokens > 0 else 0
    
    def first_word_similarity(self, name1: str, name2: str) -> float:
        """Return 100 if the first words match, otherwise 0."""
        first_word1 = name1.split()[0] if name1 else ""
        first_word2 = name2.split()[0] if name2 else ""
        return 100.0 if first_word1 == first_word2 else 0.0
    
    def calculate_composite_score(self, scores: Dict[str, float]) -> float:
        """Calculate weighted composite score from individual scores."""
        composite = 0.0
        for key, weight in self.scoring_weights.items():
            score_key = f"{key}_score"
            if score_key in scores:
                composite += weight * scores[score_key]
        return composite
    
    def match_name_to_df(self, name: str, name_original: str, row_A: pd.Series, 
                        df_B: pd.DataFrame, column_name_b: str) -> dict:
        """
        For a given processed name from dataset A, find the best match from dataset B.
        """
        result_dict = {
            "Name A Original": name_original,
            "Name A PP": name,
            "Name B Original": None,
            "Name B PP": None,
            "RapidFuzz Score": None,
            "Jellyfish Score": None,
            "TheFuzz Score": None,
            "Token-Based Score": None,
            "First-Word Score": None,
            "Partial Match Score": None,
            "Composite Score": None
        }
        
        best_match = None
        best_score = 0
        
        # Iterate over df_B using the cached preprocessed name
        for j, row_b in df_B.iterrows():
            name_b = row_b[column_name_b]
            name_b_processed = row_b.get('Name B PP', self.preprocess(name_b))
            
            # Calculate individual scores
            scores = {
                'rapidfuzz_score': fuzz.ratio(name, name_b_processed),
                'jellyfish_score': jellyfish.jaro_winkler_similarity(name, name_b_processed) * 100,
                'thefuzz_score': fuzz.token_sort_ratio(name, name_b_processed),
                'token_based_score': self.token_based_similarity(name, name_b_processed),
                'first_word_score': self.first_word_similarity(name, name_b_processed),
                'partial_match_score': fuzz.partial_ratio(name, name_b_processed)
            }
            
            composite_score = self.calculate_composite_score(scores)
            
            if composite_score > best_score:
                best_score = composite_score
                best_match = {
                    "Name B Original": name_b,
                    "Name B PP": name_b_processed,
                    "RapidFuzz Score": round(scores['rapidfuzz_score'], 1),
                    "Jellyfish Score": round(scores['jellyfish_score'], 1),
                    "TheFuzz Score": round(scores['thefuzz_score'], 1),
                    "Token-Based Score": round(scores['token_based_score'], 1),
                    "First-Word Score": round(scores['first_word_score'], 1),
                    "Partial Match Score": round(scores['partial_match_score'], 1),
                    "Composite Score": round(composite_score, 1)
                }
        
        if best_match:
            result_dict.update(best_match)
        return result_dict
    
    def find_matches_chunk(self, chunk: pd.DataFrame, df_B: pd.DataFrame, 
                          column_name_a: str, column_name_b: str) -> list:
        """Match a chunk of rows from dataset A against dataset B."""
        results = []
        for _, row in chunk.iterrows():
            name = row[column_name_a]
            name_processed = self.preprocess(name)
            results.append(self.match_name_to_df(name_processed, name, row, df_B, column_name_b))
        return results
    
    def find_matches(self, df_A: pd.DataFrame, df_B: pd.DataFrame, 
                    column_name_a: str, column_name_b: str,
                    chunk_size: int = 50, max_workers: int = 10,
                    cache_preprocessed: bool = True) -> pd.DataFrame:
        """
        Find matches using parallel processing with detailed progress tracking.
        
        Args:
            df_A: First dataframe containing names to match
            df_B: Second dataframe containing names to match against
            column_name_a: Column name in df_A containing names
            column_name_b: Column name in df_B containing names
            chunk_size: Size of chunks for parallel processing
            max_workers: Maximum number of parallel workers
            cache_preprocessed: Whether to cache preprocessed names for df_B
            
        Returns:
            DataFrame with matching results
        """
        # Cache preprocessed names for df_B if requested
        if cache_preprocessed and 'Name B PP' not in df_B.columns:
            df_B['Name B PP'] = df_B[column_name_b].apply(self.preprocess)
        
        results = []
        total_rows = len(df_A)
        chunks = [df_A.iloc[i * chunk_size:(i + 1) * chunk_size] 
                 for i in range((total_rows // chunk_size) + 1)]
        chunks = [chunk for chunk in chunks if not chunk.empty]  # Remove empty chunks
        
        print(f"Total rows: {total_rows}, processing in {len(chunks)} chunks (up to {chunk_size} rows each).")
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.find_matches_chunk, chunk, df_B, column_name_a, column_name_b): len(chunk) 
                      for chunk in chunks}
            total_processed = 0
            
            for future in as_completed(futures):
                chunk_result = future.result()
                results.extend(chunk_result)
                chunk_size_processed = futures[future]
                total_processed += chunk_size_processed
                print(f"Finished processing a chunk of {chunk_size_processed} rows. "
                      f"Total processed: {total_processed}/{total_rows} "
                      f"({(total_processed / total_rows) * 100:.2f}%).")
        
        return pd.DataFrame(results)


# Convenience functions for backward compatibility
def preprocess(name: str, stopwords: Optional[Set[str]] = None) -> str:
    """Preprocess a name using default settings."""
    matcher = NameMatcher(stopwords=stopwords)
    return matcher.preprocess(name)


def token_based_similarity(name1: str, name2: str) -> float:
    """Calculate token-based similarity between two names."""
    matcher = NameMatcher()
    return matcher.token_based_similarity(name1, name2)


def first_word_similarity(name1: str, name2: str) -> float:
    """Return 100 if the first words match, otherwise 0."""
    matcher = NameMatcher()
    return matcher.first_word_similarity(name1, name2)


def enhanced_find_matches(df_A: pd.DataFrame, df_B: pd.DataFrame, 
                         column_name_a: str, column_name_b: str,
                         chunk_size: int = 50, max_workers: int = 10,
                         stopwords: Optional[Set[str]] = None,
                         scoring_weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
    """Find matches using the NameMatcher class."""
    matcher = NameMatcher(stopwords=stopwords, scoring_weights=scoring_weights)
    return matcher.find_matches(df_A, df_B, column_name_a, column_name_b, 
                               chunk_size=chunk_size, max_workers=max_workers)