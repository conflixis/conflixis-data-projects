#!/usr/bin/env python3
"""
Healthcare Provider Disclosure Policy Review
Main analysis script for reviewing HCP disclosures against institutional policies
"""

import os
import sys
import json
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Load environment variables
load_dotenv()


class DisclosurePolicyAnalyzer:
    """Analyzes healthcare provider disclosures against institutional policies"""
    
    def __init__(self, policy_config_path: str):
        """
        Initialize the analyzer with policy configuration
        
        Args:
            policy_config_path: Path to the policy configuration YAML file
        """
        self.policies = self._load_policies(policy_config_path)
        self.results = []
        
    def _load_policies(self, config_path: str) -> Dict:
        """Load policy configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def analyze_disclosure(self, disclosure: Dict) -> Dict:
        """
        Analyze a single disclosure against policies
        
        Args:
            disclosure: Dictionary containing disclosure information
            
        Returns:
            Analysis results with compliance status and risk score
        """
        results = {
            'disclosure_id': disclosure.get('id'),
            'provider_name': disclosure.get('provider_name'),
            'disclosure_date': disclosure.get('date'),
            'violations': [],
            'warnings': [],
            'compliance_status': 'compliant',
            'risk_score': 0,
            'required_actions': []
        }
        
        # Check financial disclosures
        if 'payments' in disclosure:
            self._check_financial_policies(disclosure['payments'], results)
        
        # Check equity interests
        if 'equity' in disclosure:
            self._check_equity_policies(disclosure['equity'], results)
        
        # Check consulting agreements
        if 'consulting' in disclosure:
            self._check_consulting_policies(disclosure['consulting'], results)
        
        # Check gifts and meals
        if 'gifts' in disclosure:
            self._check_gift_policies(disclosure['gifts'], results)
        
        # Calculate risk score
        results['risk_score'] = self._calculate_risk_score(results, disclosure)
        
        # Determine overall compliance status
        results['compliance_status'] = self._determine_compliance_status(results)
        
        return results
    
    def _check_financial_policies(self, payments: List[Dict], results: Dict):
        """Check financial disclosure policies"""
        for policy in self.policies['policies']['financial_disclosures']['rules']:
            total_amount = sum(p.get('amount', 0) for p in payments)
            
            if total_amount > policy['threshold']:
                violation = {
                    'policy_id': policy['id'],
                    'policy_name': policy['name'],
                    'amount': total_amount,
                    'threshold': policy['threshold'],
                    'action': policy['action']
                }
                
                if policy['action'] in ['approval_required', 'prohibited']:
                    results['violations'].append(violation)
                else:
                    results['warnings'].append(violation)
                
                results['required_actions'].append(policy['action'])
    
    def _check_equity_policies(self, equity: List[Dict], results: Dict):
        """Check equity interest policies"""
        for policy in self.policies['policies']['equity_interests']['rules']:
            for equity_item in equity:
                value = equity_item.get('value', 0)
                
                if value > policy.get('threshold', 0):
                    violation = {
                        'policy_id': policy['id'],
                        'policy_name': policy['name'],
                        'value': value,
                        'threshold': policy.get('threshold', 0),
                        'action': policy['action']
                    }
                    
                    if policy['action'] == 'mandatory_disclosure':
                        results['required_actions'].append(policy['action'])
                    else:
                        results['warnings'].append(violation)
    
    def _check_consulting_policies(self, consulting: List[Dict], results: Dict):
        """Check consulting agreement policies"""
        for policy in self.policies['policies']['consulting_agreements']['rules']:
            if 'threshold' in policy:
                if policy.get('unit') == 'days':
                    total_days = sum(c.get('days', 0) for c in consulting)
                    if total_days > policy['threshold']:
                        results['warnings'].append({
                            'policy_id': policy['id'],
                            'policy_name': policy['name'],
                            'value': total_days,
                            'threshold': policy['threshold'],
                            'action': policy['action']
                        })
                else:
                    total_fees = sum(c.get('fee', 0) for c in consulting)
                    if total_fees > policy['threshold']:
                        results['violations'].append({
                            'policy_id': policy['id'],
                            'policy_name': policy['name'],
                            'amount': total_fees,
                            'threshold': policy['threshold'],
                            'action': policy['action']
                        })
    
    def _check_gift_policies(self, gifts: List[Dict], results: Dict):
        """Check gift and meal policies"""
        for policy in self.policies['policies']['gifts_and_meals']['rules']:
            if 'Individual' in policy['name']:
                for gift in gifts:
                    if gift.get('value', 0) > policy['threshold']:
                        results['warnings'].append({
                            'policy_id': policy['id'],
                            'policy_name': policy['name'],
                            'value': gift.get('value'),
                            'threshold': policy['threshold'],
                            'action': policy['action']
                        })
            else:
                total_value = sum(g.get('value', 0) for g in gifts)
                if total_value > policy['threshold']:
                    results['warnings'].append({
                        'policy_id': policy['id'],
                        'policy_name': policy['name'],
                        'total_value': total_value,
                        'threshold': policy['threshold'],
                        'action': policy['action']
                    })
    
    def _calculate_risk_score(self, results: Dict, disclosure: Dict) -> float:
        """Calculate risk score based on violations and disclosure details"""
        weights = self.policies['risk_scoring']['weights']
        score = 0
        
        # Factor in violations
        score += len(results['violations']) * 20
        score += len(results['warnings']) * 10
        
        # Factor in financial amounts
        if 'payments' in disclosure:
            total_payments = sum(p.get('amount', 0) for p in disclosure['payments'])
            if total_payments > 50000:
                score += 30 * weights['financial_amount']
            elif total_payments > 10000:
                score += 20 * weights['financial_amount']
            elif total_payments > 5000:
                score += 10 * weights['financial_amount']
        
        # Factor in number of relationships
        num_relationships = len(disclosure.get('payments', [])) + \
                          len(disclosure.get('equity', [])) + \
                          len(disclosure.get('consulting', []))
        if num_relationships > 10:
            score += 30 * weights['number_of_relationships']
        elif num_relationships > 5:
            score += 20 * weights['number_of_relationships']
        
        return min(score, 100)  # Cap at 100
    
    def _determine_compliance_status(self, results: Dict) -> str:
        """Determine overall compliance status"""
        if results['violations']:
            return 'non-compliant'
        elif results['warnings']:
            return 'review-required'
        else:
            return 'compliant'
    
    def generate_report(self, analysis_results: List[Dict]) -> pd.DataFrame:
        """Generate compliance report from analysis results"""
        df = pd.DataFrame(analysis_results)
        
        # Add risk categorization
        thresholds = self.policies['risk_scoring']['thresholds']
        df['risk_category'] = df['risk_score'].apply(
            lambda x: 'high' if x >= thresholds['high_risk'] 
            else 'medium' if x >= thresholds['medium_risk'] 
            else 'low'
        )
        
        return df


def main():
    """Main execution function"""
    # Initialize analyzer
    config_path = Path(__file__).parent.parent / 'config' / 'policies.yaml'
    analyzer = DisclosurePolicyAnalyzer(str(config_path))
    
    # Example disclosure (in production, this would come from database or file)
    sample_disclosure = {
        'id': 'DISC-2025-001',
        'provider_name': 'Dr. John Smith',
        'date': '2025-08-07',
        'payments': [
            {'entity': 'Pharma Co A', 'amount': 7500, 'type': 'speaking_fee'},
            {'entity': 'Medical Device B', 'amount': 3000, 'type': 'consulting'}
        ],
        'equity': [
            {'company': 'BioTech Inc', 'value': 15000, 'type': 'stock'}
        ],
        'consulting': [
            {'entity': 'Research Corp', 'fee': 20000, 'days': 15}
        ],
        'gifts': [
            {'source': 'Vendor X', 'value': 150, 'type': 'meal'}
        ]
    }
    
    # Analyze disclosure
    results = analyzer.analyze_disclosure(sample_disclosure)
    
    # Print results
    print("\\n" + "="*60)
    print("DISCLOSURE ANALYSIS RESULTS")
    print("="*60)
    print(f"Provider: {results['provider_name']}")
    print(f"Disclosure ID: {results['disclosure_id']}")
    print(f"Compliance Status: {results['compliance_status'].upper()}")
    print(f"Risk Score: {results['risk_score']:.1f}/100")
    
    if results['violations']:
        print(f"\\nViolations Found: {len(results['violations'])}")
        for v in results['violations']:
            print(f"  - {v['policy_name']}: ${v.get('amount', v.get('value', 0)):,.2f} (threshold: ${v['threshold']:,.2f})")
    
    if results['warnings']:
        print(f"\\nWarnings: {len(results['warnings'])}")
        for w in results['warnings']:
            print(f"  - {w['policy_name']}")
    
    if results['required_actions']:
        print(f"\\nRequired Actions:")
        for action in set(results['required_actions']):
            print(f"  - {action.replace('_', ' ').title()}")
    
    print("\\n" + "="*60)


if __name__ == "__main__":
    main()