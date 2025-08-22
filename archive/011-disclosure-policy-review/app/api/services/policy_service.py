"""
Policy service for handling COI policies and thresholds
"""
from typing import List, Optional, Dict, Any
from pathlib import Path
import yaml
from api.models import PolicyRule, OperationalThreshold, PolicyConfiguration


class PolicyService:
    """Service for managing policy and threshold operations"""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the policy service
        
        Args:
            config_dir: Optional path to config directory
        """
        if config_dir is None:
            # Default to the config directory relative to app
            self.config_dir = Path(__file__).parent.parent.parent.parent / "config"
        else:
            self.config_dir = Path(config_dir)
        
        self._policies: List[PolicyRule] = []
        self._thresholds: List[OperationalThreshold] = []
        self._policy_config: Optional[Dict[str, Any]] = None
        self._threshold_config: Optional[Dict[str, Any]] = None
    
    def initialize(self) -> bool:
        """Load policy and threshold configurations
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load COI policies
            policy_path = self.config_dir / "coi-policies.yaml"
            if policy_path.exists():
                with open(policy_path, 'r') as f:
                    self._policy_config = yaml.safe_load(f)
                self._parse_policies()
            
            # Load operational thresholds
            threshold_path = self.config_dir / "coi-operational-thresholds.yaml"
            if threshold_path.exists():
                with open(threshold_path, 'r') as f:
                    self._threshold_config = yaml.safe_load(f)
                self._parse_thresholds()
            
            return True
        except Exception as e:
            print(f"Failed to initialize policy service: {e}")
            return False
    
    def _parse_policies(self):
        """Parse policy configuration into PolicyRule objects"""
        if not self._policy_config:
            return
        
        self._policies = []
        
        # Parse covered persons
        for category in self._policy_config.get('covered_persons', []):
            rule = PolicyRule(
                id=f"covered_{category['category']}",
                category="covered_persons",
                description=category['description'],
                policy_clause=category['policy_clause'],
                requires_management_plan=False,
                requires_recusal=False,
                review_level="automated"
            )
            self._policies.append(rule)
        
        # Parse reportable interests
        for interest in self._policy_config.get('reportable_interests', []):
            rule = PolicyRule(
                id=f"interest_{interest['type'].replace(' ', '_')}",
                category="reportable_interests",
                description=interest['description'],
                policy_clause=interest['policy_clause'],
                requires_management_plan=interest.get('requires_management_plan', False),
                requires_recusal=interest.get('requires_recusal', False),
                review_level=interest.get('review_level', 'automated')
            )
            self._policies.append(rule)
    
    def _parse_thresholds(self):
        """Parse threshold configuration into OperationalThreshold objects"""
        if not self._threshold_config:
            return
        
        self._thresholds = []
        
        risk_tiers = self._threshold_config.get('risk_tiers', {})
        for tier_key, tier_data in risk_tiers.items():
            threshold = OperationalThreshold(
                tier=tier_key,
                label=tier_data['label'],
                range_min=tier_data['range_min'],
                range_max=tier_data['range_max'],
                description=tier_data['description'],
                management_requirements=tier_data.get('management_requirements', {}),
                typical_scenarios=tier_data.get('typical_scenarios', [])
            )
            self._thresholds.append(threshold)
    
    def get_policies(self) -> List[PolicyRule]:
        """Get all policy rules
        
        Returns:
            List of PolicyRule objects
        """
        if not self._policies:
            self.initialize()
        return self._policies
    
    def get_thresholds(self) -> List[OperationalThreshold]:
        """Get all operational thresholds
        
        Returns:
            List of OperationalThreshold objects
        """
        if not self._thresholds:
            self.initialize()
        return self._thresholds
    
    def get_policy_configuration(self) -> PolicyConfiguration:
        """Get complete policy configuration
        
        Returns:
            PolicyConfiguration object
        """
        if not self._policies or not self._thresholds:
            self.initialize()
        
        version = "1.0.0"
        last_updated = "2025-08-08"
        
        if self._policy_config:
            metadata = self._policy_config.get('metadata', {})
            version = metadata.get('version', version)
            last_updated = metadata.get('last_updated', last_updated)
        elif self._threshold_config:
            metadata = self._threshold_config.get('metadata', {})
            version = metadata.get('version', version)
            last_updated = metadata.get('last_updated', last_updated)
        
        return PolicyConfiguration(
            version=version,
            last_updated=last_updated,
            policies=self._policies,
            thresholds=self._thresholds
        )
    
    def evaluate_disclosure(self, financial_amount: float) -> Dict[str, Any]:
        """Evaluate a disclosure amount against thresholds
        
        Args:
            financial_amount: The financial amount to evaluate
            
        Returns:
            Dictionary with risk tier and requirements
        """
        if not self._thresholds:
            self.initialize()
        
        # Find matching threshold
        for threshold in self._thresholds:
            if threshold.range_min <= financial_amount <= threshold.range_max:
                return {
                    'risk_tier': threshold.tier,
                    'label': threshold.label,
                    'description': threshold.description,
                    'management_requirements': threshold.management_requirements,
                    'typical_scenarios': threshold.typical_scenarios
                }
        
        # If above all thresholds, return critical
        if financial_amount > max(t.range_max for t in self._thresholds):
            return {
                'risk_tier': 'tier_4_critical',
                'label': 'Critical Risk',
                'description': 'Exceeds all defined thresholds',
                'management_requirements': {
                    'review_level': 'committee',
                    'approval_authority': 'board_committee',
                    'management_plan': True,
                    'recusal_required': True
                },
                'typical_scenarios': ['Major equity positions', 'Board memberships']
            }
        
        # Default to low risk for small amounts
        return {
            'risk_tier': 'tier_1_low',
            'label': 'Low Risk',
            'description': 'Below reportable threshold',
            'management_requirements': {
                'review_level': 'automated',
                'approval_authority': 'system_auto_approve',
                'management_plan': False
            },
            'typical_scenarios': ['De minimis amounts']
        }
    
    def get_policy_by_clause(self, clause: str) -> Optional[PolicyRule]:
        """Find a policy by its clause reference
        
        Args:
            clause: The policy clause reference
            
        Returns:
            PolicyRule if found, None otherwise
        """
        if not self._policies:
            self.initialize()
        
        for policy in self._policies:
            if policy.policy_clause == clause:
                return policy
        
        return None