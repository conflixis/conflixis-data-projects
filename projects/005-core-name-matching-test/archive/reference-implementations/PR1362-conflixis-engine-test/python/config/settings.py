"""
Configuration settings for PR1362 matching implementation
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Model Configuration
DEFAULT_MODEL = os.getenv('DEFAULT_MODEL', 'gpt-4o-mini')
ALLOWED_MODELS = ['gpt-4o', 'gpt-4o-mini', 'gpt-4', 'gpt-3.5-turbo']

MODEL_CONFIGS = {
    'gpt-4o': {
        'name': 'gpt-4o',
        'maxTokens': 4096,
        'temperature': 0.1,
        'costPer1kTokens': 0.01  # $0.01 per 1K tokens
    },
    'gpt-4o-mini': {
        'name': 'gpt-4o-mini',
        'maxTokens': 4096,
        'temperature': 0.1,
        'costPer1kTokens': 0.0003  # $0.0003 per 1K tokens
    },
    'gpt-4': {
        'name': 'gpt-4',
        'maxTokens': 4096,
        'temperature': 0.1,
        'costPer1kTokens': 0.03  # $0.03 per 1K tokens
    },
    'gpt-3.5-turbo': {
        'name': 'gpt-3.5-turbo',
        'maxTokens': 4096,
        'temperature': 0.1,
        'costPer1kTokens': 0.0015  # $0.0015 per 1K tokens
    }
}

# Confidence Thresholds
HIGH_CONFIDENCE_THRESHOLD = 90.0  # >= this returns single match
MEDIUM_CONFIDENCE_THRESHOLD = 30.0  # >= this returns multiple matches
# Below MEDIUM_CONFIDENCE_THRESHOLD returns no match

# Fast Path - skip AI for very high confidence
FAST_PATH_THRESHOLD = 95.0

# AI Enhancement Thresholds
AI_MIN_CONFIDENCE = 30.0  # Don't use AI below this
AI_MAX_CONFIDENCE = 95.0  # Don't use AI above this

# String Similarity Weights
SIMILARITY_WEIGHTS = {
    'exact': 1.0,
    'normalized': 0.9,
    'token_sort': 0.8,
    'partial': 0.7,
    'abbreviation': 0.85,
    'typo_corrected': 0.75
}

# Context Match Weights
CONTEXT_WEIGHTS = {
    'industry': 0.15,
    'region': 0.10,
    'size': 0.05
}

# Cache Configuration
CACHE_ENABLED = True
CACHE_TTL_SECONDS = 3600  # 1 hour
MAX_CACHE_SIZE = 1000

# Search Configuration
MAX_SEARCH_RESULTS = 20
MIN_SEARCH_SCORE = 0.1  # Minimum score to consider from search

# Debug Configuration
DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# API Keys
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Batch Processing
MAX_BATCH_SIZE = 100
BATCH_CONCURRENCY = 10  # Number of concurrent API calls

# Healthcare-specific patterns
HEALTHCARE_ABBREVIATIONS = {
    'hosp': 'hospital',
    'hsptl': 'hospital',
    'med': 'medical',
    'medcl': 'medical',
    'ctr': 'center',
    'cntr': 'center',
    'hlth': 'health',
    'sys': 'system',
    'grp': 'group',
    'assoc': 'associates',
    'phys': 'physicians',
    'clinic': 'clinic',
    'clinics': 'clinics',
    'dept': 'department',
    'div': 'division',
    'inst': 'institute',
    'lab': 'laboratory',
    'labs': 'laboratories',
    'radiology': 'radiology',
    'imaging': 'imaging',
    'surgical': 'surgical',
    'surgery': 'surgery',
    'ortho': 'orthopedic',
    'cardio': 'cardiology',
    'neuro': 'neurology',
    'onc': 'oncology',
    'peds': 'pediatrics',
    'obgyn': 'obstetrics gynecology',
    'emerg': 'emergency',
    'er': 'emergency room',
    'icu': 'intensive care unit',
    'nicu': 'neonatal intensive care unit'
}

# Common healthcare system patterns
HEALTHCARE_SYSTEM_PATTERNS = [
    (r'(.+)\s+hospital$', r'\1 health system'),
    (r'(.+)\s+medical center$', r'\1 health system'),
    (r'(.+)\s+clinic$', r'\1 medical group'),
    (r'(.+)\s+physicians$', r'\1 medical group'),
    (r'(.+)\s+health$', r'\1 health system'),
    (r'st\.\s+(.+)', r'saint \1'),
    (r'mt\.\s+(.+)', r'mount \1'),
]

# Corporate suffixes to normalize
CORPORATE_SUFFIXES = [
    'inc', 'incorporated',
    'llc', 'limited liability company',
    'ltd', 'limited',
    'corp', 'corporation',
    'co', 'company',
    'pa', 'professional association',
    'pc', 'professional corporation',
    'pllc', 'professional limited liability company',
    'lp', 'limited partnership',
    'llp', 'limited liability partnership'
]