"""
Configuration for analysis thresholds and data quality checks
"""

# Analysis thresholds
ANALYSIS_THRESHOLDS = {
    'min_payment_for_influence': 1000,      # $1,000 minimum for "significant" payment
    'min_rx_for_analysis': 1000,            # $1,000 minimum for "significant" prescriber
    'max_reasonable_influence': 10,         # Flag if influence factor > 10x
    'max_reasonable_avg_rx': 5_000_000,     # Flag if avg Rx > $5M (5-year total)
    'max_reasonable_avg_rx_annual': 1_000_000  # Flag if annual avg Rx > $1M
}

# Data quality checks
DATA_QUALITY_FLAGS = {
    'high_influence_warning': 'Influence factor > 10x may indicate data quality issue',
    'high_rx_warning': 'Average Rx > $5M may indicate data aggregation issue',
    'missing_provider_type': 'Provider type unknown - not in PHYSICIANS_OVERVIEW',
    'low_prescriber_with_payments': 'Provider received payments but prescribed < $1,000'
}

# Provider type mappings
PROVIDER_TYPE_MAPPING = {
    'physician_roles': ['Physician', 'Hospitalist'],
    'nurse_practitioner_roles': ['Nurse Practitioner'],
    'physician_assistant_roles': ['Physician Assistant'],
    'advanced_practice_roles': ['Certified Registered Nurse Anesthetist', 'Certified Nurse Midwife'],
    'dentist_roles': ['Dentist']
}