"""
Core Name Matching Module

Production-ready name matching for healthcare organizations with 96.9% accuracy.
Combines fuzzy matching with AI enhancement for optimal results.
"""

# Import the production Tier 2 matcher
try:
    from .tier2_matcher import Tier2NameMatcher, match_names
except ImportError as e:
    print(f"Warning: tier2_matcher not available. Install requirements: pip install -r requirements.txt")
    print(f"Error: {e}")
    Tier2NameMatcher = None
    match_names = None

# Export only production modules
__all__ = []
if Tier2NameMatcher:
    __all__.extend(['Tier2NameMatcher', 'match_names'])

# Module metadata
__version__ = '3.0.0'
__author__ = 'Conflixis Data Team'
__description__ = 'Production-ready healthcare entity name matching with AI enhancement (96.9% accuracy)'

# Default configuration for Tier 2 (production)
TIER2_CONFIG = {
    'fuzzy_threshold': 85.0,     # Threshold for fuzzy-only matching
    'decision_threshold': 50.0,   # Final decision threshold
    'model': 'gpt-4o-mini',      # Recommended OpenAI model
    'max_workers': 5              # Concurrent API workers
}

# Quick start function
def create_matcher(**kwargs):
    """
    Create a configured Tier 2 matcher instance.
    
    Args:
        **kwargs: Override default configuration
        
    Returns:
        Tier2NameMatcher instance or None if not available
    """
    if not Tier2NameMatcher:
        raise ImportError("Tier2NameMatcher not available. Install requirements first.")
    
    config = TIER2_CONFIG.copy()
    config.update(kwargs)
    return Tier2NameMatcher(**config)

if create_matcher:
    __all__.append('create_matcher')