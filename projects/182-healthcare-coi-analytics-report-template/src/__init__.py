"""Healthcare COI Analytics Template - Core Package"""

__version__ = "2.0.0"
__author__ = "Conflixis Analytics Team"

from .data import BigQueryConnector, DataLoader, DataValidator
from .analysis import OpenPaymentsAnalyzer, PrescriptionAnalyzer, CorrelationAnalyzer, RiskScorer
from .reporting import ReportGenerator

__all__ = [
    'BigQueryConnector',
    'DataLoader', 
    'DataValidator',
    'OpenPaymentsAnalyzer',
    'PrescriptionAnalyzer',
    'CorrelationAnalyzer',
    'RiskScorer',
    'ReportGenerator'
]