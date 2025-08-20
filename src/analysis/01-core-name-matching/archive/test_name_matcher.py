"""
Unit tests for the name matching modules.
Run with: pytest test_name_matcher.py
"""

import pytest
import pandas as pd
import numpy as np
from typing import Dict, List

# Import both versions of the matcher
from name_matcher import NameMatcher, preprocess, token_based_similarity, first_word_similarity
from name_matcher_v2 import EnhancedNameMatcher, MatchResult


class TestPreprocessing:
    """Test preprocessing functions."""
    
    def test_basic_preprocessing(self):
        """Test basic name preprocessing."""
        matcher = NameMatcher()
        
        # Test lowercase conversion
        assert matcher.preprocess("HOSPITAL NAME") == "hospital name"
        
        # Test special character removal
        assert matcher.preprocess("St. Mary's Hospital") == "st marys hospital"
        
        # Test abbreviation expansion
        assert matcher.preprocess("ABC Corp.") == "abc corporation"
        assert matcher.preprocess("XYZ Inc.") == "xyz inc"
        
        # Test empty and None handling
        assert matcher.preprocess("") == ""
        assert matcher.preprocess(None) == ""
        assert matcher.preprocess(123) == ""
    
    def test_stopword_removal(self):
        """Test stopword removal in preprocessing."""
        stopwords = {'the', 'and', 'of', 'inc', 'corporation'}
        matcher = NameMatcher(stopwords=stopwords)
        
        assert matcher.preprocess("The Hospital of Angels") == "hospital angels"
        assert matcher.preprocess("ABC and XYZ Corporation") == "abc xyz"
    
    def test_enhanced_preprocessing(self):
        """Test enhanced preprocessing with more abbreviations."""
        matcher = EnhancedNameMatcher()
        
        assert matcher.preprocess("ABC LLC") == "abc limited liability company"
        assert matcher.preprocess("XYZ Ltd.") == "xyz limited"


class TestSimilarityFunctions:
    """Test individual similarity functions."""
    
    def test_token_based_similarity(self):
        """Test token-based similarity calculation."""
        matcher = NameMatcher()
        
        # Exact match
        assert matcher.token_based_similarity("abc hospital", "abc hospital") == 100.0
        
        # Partial match
        assert matcher.token_based_similarity("abc hospital", "abc clinic") == 50.0
        
        # No match
        assert matcher.token_based_similarity("abc hospital", "xyz clinic") == 0.0
        
        # Different word order
        assert matcher.token_based_similarity("hospital abc", "abc hospital") == 100.0
        
        # Empty strings
        assert matcher.token_based_similarity("", "") == 0.0
        assert matcher.token_based_similarity("abc", "") == 0.0
    
    def test_first_word_similarity(self):
        """Test first word similarity function."""
        matcher = NameMatcher()
        
        # Matching first words
        assert matcher.first_word_similarity("abc hospital", "abc clinic") == 100.0
        
        # Different first words
        assert matcher.first_word_similarity("abc hospital", "xyz hospital") == 0.0
        
        # Empty strings
        assert matcher.first_word_similarity("", "") == 0.0
        assert matcher.first_word_similarity("abc", "") == 0.0


class TestCompositeScoring:
    """Test composite score calculation."""
    
    def test_composite_score_calculation(self):
        """Test weighted composite score calculation."""
        matcher = EnhancedNameMatcher()
        
        scores = {
            'rapidfuzz': 80.0,
            'jellyfish': 90.0,
            'thefuzz': 70.0,
            'token_based': 100.0,
            'first_word': 100.0,
            'partial_match': 85.0
        }
        
        # Expected: 0.1*80 + 0.2*90 + 0.1*70 + 0.1*100 + 0.4*100 + 0.1*85 = 91.5
        expected = 91.5
        assert abs(matcher.calculate_composite_score(scores) - expected) < 0.01
    
    def test_custom_weights(self):
        """Test composite score with custom weights."""
        custom_weights = {
            'rapidfuzz': 0.2,
            'jellyfish': 0.2,
            'thefuzz': 0.2,
            'token_based': 0.2,
            'first_word': 0.1,
            'partial_match': 0.1
        }
        
        matcher = EnhancedNameMatcher(scoring_weights=custom_weights)
        
        scores = {
            'rapidfuzz': 100.0,
            'jellyfish': 0.0,
            'thefuzz': 100.0,
            'token_based': 0.0,
            'first_word': 100.0,
            'partial_match': 0.0
        }
        
        # Expected: 0.2*100 + 0.2*0 + 0.2*100 + 0.2*0 + 0.1*100 + 0.1*0 = 50.0
        assert matcher.calculate_composite_score(scores) == 50.0


class TestConfidenceLevels:
    """Test confidence level assignment."""
    
    def test_confidence_levels(self):
        """Test confidence level determination."""
        matcher = EnhancedNameMatcher()
        
        assert matcher.get_confidence_level(90.0) == 'High'
        assert matcher.get_confidence_level(80.0) == 'High'
        assert matcher.get_confidence_level(70.0) == 'Medium'
        assert matcher.get_confidence_level(60.0) == 'Medium'
        assert matcher.get_confidence_level(50.0) == 'Low'
        assert matcher.get_confidence_level(40.0) == 'Low'
        assert matcher.get_confidence_level(30.0) == 'No Match'
        assert matcher.get_confidence_level(0.0) == 'No Match'
    
    def test_custom_confidence_thresholds(self):
        """Test custom confidence thresholds."""
        custom_thresholds = {
            'high': 90.0,
            'medium': 70.0,
            'low': 50.0
        }
        
        matcher = EnhancedNameMatcher(confidence_thresholds=custom_thresholds)
        
        assert matcher.get_confidence_level(95.0) == 'High'
        assert matcher.get_confidence_level(85.0) == 'Medium'
        assert matcher.get_confidence_level(60.0) == 'Low'
        assert matcher.get_confidence_level(40.0) == 'No Match'


class TestThresholdBehavior:
    """Test minimum score threshold behavior."""
    
    def test_no_match_below_threshold(self):
        """Test that matches below threshold return 'No Match'."""
        # Create test dataframes
        df_a = pd.DataFrame({
            'name': ['ABC Hospital']
        })
        
        df_b = pd.DataFrame({
            'Name': ['XYZ Clinic', 'DEF Medical Center']
        })
        
        # Set high threshold so no matches are found
        matcher = EnhancedNameMatcher(min_score_threshold=90.0)
        
        matches = matcher.find_top_n_matches(
            'abc hospital', 'ABC Hospital', df_b, 'Name'
        )
        
        assert len(matches) == 1
        assert matches[0].confidence_level == 'No Match'
        assert matches[0].name_b_original is None
        assert matches[0].composite_score == 0.0


class TestTopNMatches:
    """Test top-N match functionality."""
    
    def test_return_multiple_matches(self):
        """Test returning multiple top matches."""
        df_b = pd.DataFrame({
            'Name': [
                'ABC Hospital',
                'ABC Medical Center',
                'ABC Clinic',
                'XYZ Hospital'
            ]
        })
        
        # Preprocess names for efficiency
        matcher = EnhancedNameMatcher(return_top_n=3, min_score_threshold=0.0)
        df_b['Name B PP'] = df_b['Name'].apply(matcher.preprocess)
        
        matches = matcher.find_top_n_matches(
            'abc hospital', 'ABC Hospital', df_b, 'Name'
        )
        
        # Should return 3 matches
        assert len(matches) == 3
        
        # Verify ranking
        assert all(matches[i].composite_score >= matches[i+1].composite_score 
                  for i in range(len(matches)-1))
        
        # Verify ranks are assigned correctly
        assert matches[0].rank == 1
        assert matches[1].rank == 2
        assert matches[2].rank == 3


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_dataframes(self):
        """Test behavior with empty dataframes."""
        df_a = pd.DataFrame({'name': []})
        df_b = pd.DataFrame({'Name': []})
        
        matcher = EnhancedNameMatcher()
        
        # Should return empty DataFrame without errors
        results = matcher.find_matches(df_a, df_b, 'name', 'Name')
        assert len(results) == 0
    
    def test_missing_columns(self):
        """Test error handling for missing columns."""
        df_a = pd.DataFrame({'wrong_column': ['ABC']})
        df_b = pd.DataFrame({'Name': ['XYZ']})
        
        matcher = EnhancedNameMatcher()
        
        with pytest.raises(ValueError, match="Column 'name' not found"):
            matcher.find_matches(df_a, df_b, 'name', 'Name')
    
    def test_special_characters_in_names(self):
        """Test handling of special characters."""
        matcher = EnhancedNameMatcher()
        
        # Test various special characters
        assert matcher.preprocess("St. Mary's Hospital-North") == "st marys hospital-north"
        assert matcher.preprocess("ABC (formerly XYZ)") == "abc formerly xyz"
        assert matcher.preprocess("Hospital #1") == "hospital 1"
        assert matcher.preprocess("A&B Healthcare") == "ab healthcare"


class TestIntegration:
    """Integration tests for the complete matching process."""
    
    def test_full_matching_process(self):
        """Test the complete matching workflow."""
        # Create test data
        df_a = pd.DataFrame({
            'hospital_name': [
                'Mercy Hospital',
                'St. Johns Medical Center',
                'City General Hospital'
            ]
        })
        
        df_b = pd.DataFrame({
            'facility_name': [
                'Mercy Hospital System',
                'Saint Johns Med Ctr',
                'General Hospital of the City',
                'Unrelated Clinic'
            ]
        })
        
        # Configure matcher
        matcher = EnhancedNameMatcher(
            min_score_threshold=50.0,
            return_top_n=2
        )
        
        # Run matching
        results = matcher.find_matches(
            df_a, df_b,
            'hospital_name', 'facility_name',
            chunk_size=2,
            max_workers=1
        )
        
        # Verify results structure
        assert len(results) > 0
        assert 'hospital_name' in results.columns
        assert 'facility_name_match_1' in results.columns
        assert 'Composite Score_match_1' in results.columns
        assert 'Confidence_match_1' in results.columns
        
        # Each input name should have results
        unique_names = results['hospital_name'].unique()
        assert len(unique_names) == len(df_a)


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])