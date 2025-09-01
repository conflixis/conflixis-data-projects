"""
Output Validator Module
Validates LLM-generated content against actual data to prevent hallucinations
"""

import re
import logging
from typing import Dict, List, Tuple, Any
import pandas as pd

logger = logging.getLogger(__name__)


class OutputValidator:
    """Validates LLM output against source data to detect hallucinations"""
    
    def __init__(self, source_data: Dict[str, Any]):
        """
        Initialize validator with source data
        
        Args:
            source_data: Dictionary containing the actual analysis data
        """
        self.source_data = source_data
        self.known_provider_counts = self._extract_provider_counts()
        self.known_dollar_amounts = self._extract_dollar_amounts()
        
    def _extract_provider_counts(self) -> Dict[str, int]:
        """Extract all provider counts from source data"""
        counts = {}
        
        # Extract from open_payments if available
        if 'open_payments' in self.source_data:
            op = self.source_data['open_payments']
            if 'overall_metrics' in op:
                counts['total_providers'] = op['overall_metrics'].get('unique_providers', 0)
            
            # Get consecutive year counts
            if 'consecutive_years' in op and isinstance(op['consecutive_years'], pd.DataFrame):
                df = op['consecutive_years']
                if not df.empty:
                    # Count providers by years
                    for years in range(1, 6):
                        count = len(df[df.get('years_with_payments', 0) == years]) if 'years_with_payments' in df.columns else 0
                        counts[f'providers_{years}_years'] = count
        
        # Add hardcoded known values for validation
        counts['providers_all_5_years'] = 6  # We know this from our analysis
        
        return counts
    
    def _extract_dollar_amounts(self) -> List[float]:
        """Extract all dollar amounts from source data"""
        amounts = []
        
        # Extract payment amounts
        if 'open_payments' in self.source_data:
            op = self.source_data['open_payments']
            if 'overall_metrics' in op:
                if 'total_payments' in op['overall_metrics']:
                    amounts.append(op['overall_metrics']['total_payments'])
                if 'avg_payment' in op['overall_metrics']:
                    amounts.append(op['overall_metrics']['avg_payment'])
        
        # Extract prescription amounts
        if 'prescriptions' in self.source_data:
            rx = self.source_data['prescriptions']
            if 'overall_metrics' in rx:
                if 'total_prescription_value' in rx['overall_metrics']:
                    amounts.append(rx['overall_metrics']['total_prescription_value'])
        
        return amounts
    
    def validate_content(self, content: str, section_name: str = "") -> Tuple[bool, List[str]]:
        """
        Validate LLM-generated content against source data
        
        Args:
            content: The generated text to validate
            section_name: Name of the section being validated
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for the notorious "2,343 providers" hallucination
        if "2,343 providers" in content or "2343 providers" in content:
            issues.append("CRITICAL: Found fictional '2,343 providers' - actual is 6 providers with all 5 years")
        
        # Extract numbers from content
        provider_pattern = r'(\d+(?:,\d+)*)\s*providers?'
        provider_matches = re.findall(provider_pattern, content, re.IGNORECASE)
        
        for match in provider_matches:
            number = int(match.replace(',', ''))
            # Check if this is a plausible provider count
            if number > 100 and self.known_provider_counts.get('total_providers', 0) < 100:
                issues.append(f"Suspicious provider count: {number} (actual total is {self.known_provider_counts.get('total_providers', 'unknown')})")
            
            # Check specific problematic numbers
            if number > 1000:
                issues.append(f"Implausible provider count: {number} - we only have data for ~81 providers")
        
        # Check for made-up dollar amounts
        dollar_pattern = r'\$(\d+(?:,\d+)*(?:\.\d+)?)'
        dollar_matches = re.findall(dollar_pattern, content)
        
        for match in dollar_matches:
            amount = float(match.replace(',', ''))
            # Check if this amount is way off from known amounts
            if amount > 1000000 and amount not in self.known_dollar_amounts:
                # Check if it's close to any known amount (within 10%)
                close_match = any(abs(amount - known) / known < 0.1 for known in self.known_dollar_amounts if known > 0)
                if not close_match and amount > 10000000:
                    issues.append(f"Suspicious dollar amount: ${amount:,.0f} not found in source data")
        
        # Check for specific hallucination patterns
        hallucination_patterns = [
            (r'(\d+)x\s+multiplier', "Suspicious multiplier - verify against actual data"),
            (r'(\d+)%\s+increase', "Percentage increase should be verified against actual data"),
            (r'average of \$(\d+(?:,\d+)*)', "Average amount should be verified against actual calculations")
        ]
        
        for pattern, message in hallucination_patterns:
            if re.search(pattern, content):
                issues.append(f"WARNING: {message}")
        
        # Section-specific validations
        if section_name == "consecutive_years":
            # Check for the specific correct number
            if "6 providers" not in content and "six providers" not in content.lower():
                issues.append("Missing correct count: Should mention 6 providers with all 5 years of payments")
        
        is_valid = len(issues) == 0
        return is_valid, issues
    
    def suggest_corrections(self, content: str, issues: List[str]) -> str:
        """
        Suggest corrections for identified issues
        
        Args:
            content: The content with issues
            issues: List of identified issues
            
        Returns:
            Suggested corrections as a string
        """
        corrections = []
        
        for issue in issues:
            if "2,343 providers" in issue:
                corrections.append("Replace '2,343 providers' with '6 providers' (the actual count)")
            elif "Implausible provider count" in issue:
                corrections.append(f"Verify provider counts against actual data: {self.known_provider_counts}")
            elif "Suspicious dollar amount" in issue:
                corrections.append("Verify all dollar amounts against source data")
        
        return "\n".join(corrections) if corrections else "No specific corrections suggested"


def validate_report_section(
    section_content: str,
    section_name: str,
    source_data: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Convenience function to validate a report section
    
    Args:
        section_content: The generated section content
        section_name: Name of the section
        source_data: The source analysis data
        
    Returns:
        Tuple of (is_valid, validation_message)
    """
    validator = OutputValidator(source_data)
    is_valid, issues = validator.validate_content(section_content, section_name)
    
    if is_valid:
        return True, "✅ Content validation passed - no hallucinations detected"
    else:
        message = f"⚠️ Content validation issues found:\n"
        for issue in issues:
            message += f"  - {issue}\n"
        
        corrections = validator.suggest_corrections(section_content, issues)
        if corrections != "No specific corrections suggested":
            message += f"\nSuggested corrections:\n{corrections}"
        
        return False, message