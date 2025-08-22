"""
Policy Decision Engine for COI Compliance
Implements DA-151: Create Policy Decision Engine for COI Compliance

This engine processes disclosures against the COI policy rules defined in coi-policies.yaml
and operational thresholds defined in coi-operational-thresholds.yaml.
"""
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
import yaml
import json
from enum import Enum


class RiskLevel(Enum):
    """Risk level classifications"""
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class ConflictType(Enum):
    """Types of conflicts identified"""
    NONE = "none"
    MANAGEABLE = "manageable"
    INSURMOUNTABLE = "insurmountable"


@dataclass
class PolicyDecision:
    """Represents a policy decision for a disclosure"""
    disclosure_id: str
    provider_name: str
    provider_npi: str
    risk_level: RiskLevel
    conflict_type: ConflictType
    policy_violations: List[str] = field(default_factory=list)
    required_actions: List[str] = field(default_factory=list)
    management_plan_required: bool = False
    recusal_required: bool = False
    approval_authority: str = "system_auto_approve"
    review_level: str = "automated"
    monitoring_frequency: str = "annual"
    policy_clauses_triggered: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    risk_score: float = 0.0
    decision_timestamp: datetime = field(default_factory=datetime.now)
    decision_rationale: str = ""
    next_review_date: Optional[datetime] = None


class PolicyEngine:
    """
    Core Policy Decision Engine for evaluating COI disclosures
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the policy engine
        
        Args:
            config_dir: Optional path to config directory
        """
        if config_dir is None:
            self.config_dir = Path(__file__).parent.parent.parent.parent / "config"
        else:
            self.config_dir = Path(config_dir)
        
        self.policies = {}
        self.thresholds = {}
        self.policy_mappings = {}
        self._load_configurations()
    
    def _load_configurations(self) -> bool:
        """Load policy and threshold configurations
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load COI policies
            policy_path = self.config_dir / "coi-policies.yaml"
            if policy_path.exists():
                with open(policy_path, 'r') as f:
                    self.policies = yaml.safe_load(f)
            
            # Load operational thresholds
            threshold_path = self.config_dir / "coi-operational-thresholds.yaml"
            if threshold_path.exists():
                with open(threshold_path, 'r') as f:
                    self.thresholds = yaml.safe_load(f)
                    # Extract policy mappings
                    self.policy_mappings = self.thresholds.get('integration', {}).get('policy_references', {})
            
            return True
        except Exception as e:
            print(f"Failed to load configurations: {e}")
            return False
    
    def evaluate_disclosure(self, disclosure: Dict[str, Any]) -> PolicyDecision:
        """
        Evaluate a disclosure against policy rules and generate a decision
        
        Args:
            disclosure: Dictionary containing disclosure data
            
        Returns:
            PolicyDecision object with evaluation results
        """
        # Initialize decision with default values
        decision = PolicyDecision(
            disclosure_id=disclosure.get('id', 'unknown'),
            provider_name=disclosure.get('member_name', disclosure.get('provider_name', 'Unknown')),
            provider_npi=disclosure.get('provider_npi', 'Unknown'),
            risk_level=RiskLevel.LOW,  # Default, will be updated
            conflict_type=ConflictType.NONE  # Default, will be updated
        )
        
        # Step 1: Check for insurmountable conflicts
        insurmountable = self._check_insurmountable_conflicts(disclosure)
        if insurmountable:
            decision.conflict_type = ConflictType.INSURMOUNTABLE
            decision.risk_level = RiskLevel.CRITICAL
            decision.policy_violations = insurmountable
            decision.required_actions = ["Immediate board review required", "Consider termination or divestiture"]
            decision.policy_clauses_triggered = ["4.8"]
            decision.decision_rationale = "Insurmountable conflict identified per Policy Section 4.8"
            return decision
        
        # Step 2: Calculate risk score
        risk_score = self._calculate_risk_score(disclosure)
        decision.risk_score = risk_score
        
        # Step 3: Determine risk tier based on financial amount
        financial_amount = disclosure.get('amount', disclosure.get('financial_amount', 0))
        risk_tier = self._determine_risk_tier(financial_amount)
        
        # Step 4: Apply risk tier management requirements
        self._apply_risk_tier_requirements(decision, risk_tier, disclosure)
        
        # Step 5: Check for specific policy violations
        violations = self._check_policy_violations(disclosure)
        if violations:
            decision.policy_violations.extend(violations)
            decision.conflict_type = ConflictType.MANAGEABLE
        
        # Step 6: Generate recommendations
        recommendations = self._generate_recommendations(disclosure, decision)
        decision.recommendations = recommendations
        
        # Step 7: Set decision rationale
        decision.decision_rationale = self._generate_rationale(disclosure, decision)
        
        return decision
    
    def _check_insurmountable_conflicts(self, disclosure: Dict[str, Any]) -> List[str]:
        """
        Check for insurmountable conflicts per Policy Section 4.8
        
        Args:
            disclosure: Disclosure data
            
        Returns:
            List of insurmountable conflicts found
        """
        conflicts = []
        insurmountable_section = self.policies.get('insurmountable_conflicts', [])
        
        entity_name = disclosure.get('organization', disclosure.get('entity_name', '')).lower()
        relationship_type = disclosure.get('relationship_type', '').lower()
        
        for conflict in insurmountable_section:
            conflict_type = conflict.get('type', '')
            
            # Check for major supplier relationship (4.8.1)
            if conflict_type == 'major_supplier_relationship':
                if 'supplier' in entity_name or 'vendor' in entity_name:
                    if disclosure.get('amount', disclosure.get('financial_amount', 0)) > 100000:
                        conflicts.append(f"Major supplier relationship (Section {conflict.get('policy_clause')})")
            
            # Check for elected officials (4.8.2)
            elif conflict_type == 'elected_officials':
                if 'government' in entity_name or 'senator' in relationship_type or 'representative' in relationship_type:
                    conflicts.append(f"Elected official relationship (Section {conflict.get('policy_clause')})")
            
            # Check for competing healthcare (4.8.3)
            elif conflict_type == 'competing_healthcare':
                competing_keywords = ['competing', 'competitor', 'rival hospital', 'other health system']
                if any(keyword in entity_name.lower() for keyword in competing_keywords):
                    conflicts.append(f"Competing healthcare entity (Section {conflict.get('policy_clause')})")
            
            # Check for mission conflict (4.8.4)
            elif conflict_type == 'mission_conflict':
                mission_conflict_keywords = ['tobacco', 'gambling', 'firearms']
                if any(keyword in entity_name.lower() for keyword in mission_conflict_keywords):
                    conflicts.append(f"Mission conflict (Section {conflict.get('policy_clause')})")
        
        return conflicts
    
    def _calculate_risk_score(self, disclosure: Dict[str, Any]) -> float:
        """
        Calculate weighted risk score based on multiple factors
        
        Args:
            disclosure: Disclosure data
            
        Returns:
            Risk score (0-100)
        """
        scoring_config = self.thresholds.get('risk_scoring', {})
        weights = scoring_config.get('factor_weights', {})
        
        score = 0.0
        
        # Financial amount component
        financial_amount = disclosure.get('amount', disclosure.get('financial_amount', 0))
        financial_score = self._score_financial_amount(financial_amount, scoring_config)
        score += financial_score * (weights.get('financial_amount', 40) / 100)
        
        # Decision authority component
        job_title = disclosure.get('job_title', '').lower()
        authority_score = self._score_decision_authority(job_title, scoring_config)
        score += authority_score * (weights.get('decision_authority', 25) / 100)
        
        # Relationship type component
        relationship_type = disclosure.get('relationship_type', '').lower()
        relationship_score = self._score_relationship_type(relationship_type, scoring_config)
        score += relationship_score * (weights.get('relationship_type', 20) / 100)
        
        # Service overlap component
        entity_name = disclosure.get('organization', disclosure.get('entity_name', '')).lower()
        overlap_score = self._score_service_overlap(entity_name, scoring_config)
        score += overlap_score * (weights.get('service_overlap', 10) / 100)
        
        # Frequency component
        frequency_score = self._score_frequency(disclosure, scoring_config)
        score += frequency_score * (weights.get('frequency', 5) / 100)
        
        return min(100, max(0, score))
    
    def _score_financial_amount(self, amount: float, config: Dict) -> float:
        """Score financial amount component"""
        scale = config.get('financial_amount_scale', {})
        
        if amount <= 1000:
            return scale.get('minimal', {}).get('score', 5)
        elif amount <= 5000:
            return scale.get('low', {}).get('score', 20)
        elif amount <= 25000:
            return scale.get('moderate', {}).get('score', 40)
        elif amount <= 100000:
            return scale.get('high', {}).get('score', 70)
        else:
            return scale.get('critical', {}).get('score', 100)
    
    def _score_decision_authority(self, job_title: str, config: Dict) -> float:
        """Score decision authority based on job title"""
        scale = config.get('decision_authority_scale', {})
        
        if any(term in job_title for term in ['board', 'trustee']):
            return scale.get('board_level', {}).get('score', 100)
        elif any(term in job_title for term in ['chief', 'president', 'vp', 'vice president']):
            return scale.get('executive_level', {}).get('score', 75)
        elif any(term in job_title for term in ['director', 'manager', 'supervisor']):
            return scale.get('manager_level', {}).get('score', 50)
        elif any(term in job_title for term in ['staff', 'coordinator', 'specialist']):
            return scale.get('staff_level', {}).get('score', 25)
        else:
            return scale.get('no_authority', {}).get('score', 0)
    
    def _score_relationship_type(self, relationship_type: str, config: Dict) -> float:
        """Score relationship type"""
        scale = config.get('relationship_type_scale', {})
        
        if 'family' in relationship_type or 'spouse' in relationship_type:
            return scale.get('indirect', {}).get('score', 30)
        elif 'investment' in relationship_type or 'ownership' in relationship_type:
            return scale.get('passive', {}).get('score', 40)
        elif 'consulting' in relationship_type or 'advisory' in relationship_type:
            return scale.get('active_limited', {}).get('score', 60)
        elif 'board' in relationship_type or 'executive' in relationship_type:
            return scale.get('active_significant', {}).get('score', 100)
        else:
            return 50  # Default moderate score
    
    def _score_service_overlap(self, entity_name: str, config: Dict) -> float:
        """Score service overlap with entity"""
        scale = config.get('service_overlap_scale', {})
        
        if 'competitor' in entity_name or 'competing' in entity_name:
            return scale.get('direct_competition', {}).get('score', 100)
        elif 'healthcare' in entity_name or 'medical' in entity_name or 'hospital' in entity_name:
            return scale.get('significant', {}).get('score', 75)
        elif 'pharma' in entity_name or 'device' in entity_name:
            return scale.get('moderate', {}).get('score', 50)
        else:
            return scale.get('minimal', {}).get('score', 25)
    
    def _score_frequency(self, disclosure: Dict, config: Dict) -> float:
        """Score relationship frequency"""
        scale = config.get('frequency_scale', {})
        
        # This would ideally check historical data
        # For now, use relationship type as proxy
        relationship_type = disclosure.get('relationship_type', '').lower()
        
        if 'ongoing' in relationship_type or 'continuous' in relationship_type:
            return scale.get('ongoing', {}).get('score', 100)
        elif 'regular' in relationship_type or 'monthly' in relationship_type:
            return scale.get('regular', {}).get('score', 70)
        elif 'occasional' in relationship_type or 'quarterly' in relationship_type:
            return scale.get('occasional', {}).get('score', 40)
        else:
            return scale.get('one_time', {}).get('score', 20)
    
    def _determine_risk_tier(self, financial_amount: float) -> Dict[str, Any]:
        """
        Determine risk tier based on financial amount
        
        Args:
            financial_amount: Financial amount to evaluate
            
        Returns:
            Risk tier configuration
        """
        risk_tiers = self.thresholds.get('risk_tiers', {})
        
        # Check each tier
        for tier_key in ['tier_1_low', 'tier_2_moderate', 'tier_3_high', 'tier_4_critical']:
            tier = risk_tiers.get(tier_key, {})
            range_min = tier.get('range_min', 0)
            range_max = tier.get('range_max', float('inf'))
            
            if range_min <= financial_amount <= (range_max or float('inf')):
                return tier
        
        # Default to low risk
        return risk_tiers.get('tier_1_low', {})
    
    def _apply_risk_tier_requirements(self, decision: PolicyDecision, tier: Dict, disclosure: Dict):
        """Apply management requirements based on risk tier"""
        management_req = tier.get('management_requirements', {})
        
        # Set risk level
        label = tier.get('label', 'Low Risk')
        if 'Critical' in label:
            decision.risk_level = RiskLevel.CRITICAL
        elif 'High' in label:
            decision.risk_level = RiskLevel.HIGH
        elif 'Moderate' in label:
            decision.risk_level = RiskLevel.MODERATE
        else:
            decision.risk_level = RiskLevel.LOW
        
        # Apply management requirements
        decision.review_level = management_req.get('review_level', 'automated')
        decision.approval_authority = management_req.get('approval_authority', 'system_auto_approve')
        decision.management_plan_required = management_req.get('management_plan', False)
        decision.monitoring_frequency = management_req.get('monitoring_frequency', 'annual')
        
        # Add policy clause reference
        if tier.get('policy_clause'):
            decision.policy_clauses_triggered.append(tier.get('policy_clause'))
        
        # Add additional requirements
        additional = management_req.get('additional_requirements', [])
        decision.required_actions.extend(additional)
        
        # Check if recusal is required
        if decision.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            if disclosure.get('financial_amount', 0) > 50000:
                decision.recusal_required = True
                decision.required_actions.append("Recusal from related decisions required")
    
    def _check_policy_violations(self, disclosure: Dict) -> List[str]:
        """Check for specific policy violations"""
        violations = []
        
        # Check 1% ownership rule (Section 5.9)
        if disclosure.get('equity_percentage', 0) >= 1:
            if disclosure.get('publicly_traded', False):
                violations.append("Exceeds 1% ownership in publicly traded company (Section 5.9)")
        
        # Check compensation restrictions (Section 4.12)
        if 'committee' in disclosure.get('job_title', '').lower():
            if disclosure.get('receives_compensation', False):
                violations.append("Committee member with compensation conflict (Section 4.12.1)")
        
        # Check disclosure timing (Section 4.4)
        if disclosure.get('days_since_awareness', 0) > 30:
            violations.append("Delayed disclosure beyond immediate requirement (Section 4.4)")
        
        return violations
    
    def _generate_recommendations(self, disclosure: Dict, decision: PolicyDecision) -> List[str]:
        """Generate specific recommendations based on the evaluation"""
        recommendations = []
        
        # Based on risk level
        if decision.risk_level == RiskLevel.CRITICAL:
            recommendations.append("Immediate board notification required")
            recommendations.append("Consider divestiture or resignation from external position")
            recommendations.append("Legal counsel consultation recommended")
        elif decision.risk_level == RiskLevel.HIGH:
            recommendations.append("Develop comprehensive management plan within 30 days")
            recommendations.append("Quarterly monitoring and attestation required")
            recommendations.append("Disclosure to affected departments")
        elif decision.risk_level == RiskLevel.MODERATE:
            recommendations.append("Document mitigation measures")
            recommendations.append("Semi-annual review by manager")
            recommendations.append("Update disclosure if circumstances change")
        else:
            recommendations.append("Annual attestation required")
            recommendations.append("No active management required at this time")
        
        # Specific to relationship type
        relationship_type = disclosure.get('relationship_type', '').lower()
        if 'research' in relationship_type:
            recommendations.append("Research compliance review required")
        if 'speaking' in relationship_type:
            recommendations.append("Speaking engagement guidelines must be followed")
        if 'family' in relationship_type or 'related' in relationship_type:
            recommendations.append("Family member relationships require enhanced documentation")
        
        return recommendations
    
    def _generate_rationale(self, disclosure: Dict, decision: PolicyDecision) -> str:
        """Generate decision rationale"""
        rationale_parts = []
        
        # Risk assessment
        rationale_parts.append(f"Risk Assessment: {decision.risk_level.value.capitalize()} risk (score: {decision.risk_score:.1f}/100)")
        
        # Financial component
        amount = disclosure.get('financial_amount', 0)
        if amount > 0:
            rationale_parts.append(f"Financial Interest: ${amount:,.2f}")
        
        # Conflict type
        if decision.conflict_type != ConflictType.NONE:
            rationale_parts.append(f"Conflict Type: {decision.conflict_type.value}")
        
        # Policy sections triggered
        if decision.policy_clauses_triggered:
            clauses = ", ".join(decision.policy_clauses_triggered)
            rationale_parts.append(f"Policy Sections: {clauses}")
        
        # Management requirements
        if decision.management_plan_required:
            rationale_parts.append("Management plan required per policy")
        
        if decision.recusal_required:
            rationale_parts.append("Recusal from related decisions required")
        
        return " | ".join(rationale_parts)
    
    def batch_evaluate(self, disclosures: List[Dict[str, Any]]) -> List[PolicyDecision]:
        """
        Evaluate multiple disclosures
        
        Args:
            disclosures: List of disclosure dictionaries
            
        Returns:
            List of PolicyDecision objects
        """
        decisions = []
        for disclosure in disclosures:
            decision = self.evaluate_disclosure(disclosure)
            decisions.append(decision)
        return decisions
    
    def generate_review_document(self, decision: PolicyDecision) -> Dict[str, Any]:
        """
        Generate a comprehensive review document for a disclosure
        
        Args:
            decision: PolicyDecision object
            
        Returns:
            Dictionary containing the review document
        """
        return {
            "disclosure_id": decision.disclosure_id,
            "provider": {
                "name": decision.provider_name,
                "npi": decision.provider_npi
            },
            "assessment": {
                "risk_level": decision.risk_level.value,
                "risk_score": decision.risk_score,
                "conflict_type": decision.conflict_type.value,
                "decision_timestamp": decision.decision_timestamp.isoformat()
            },
            "compliance": {
                "policy_violations": decision.policy_violations,
                "policy_clauses_triggered": decision.policy_clauses_triggered,
                "management_plan_required": decision.management_plan_required,
                "recusal_required": decision.recusal_required
            },
            "requirements": {
                "required_actions": decision.required_actions,
                "approval_authority": decision.approval_authority,
                "review_level": decision.review_level,
                "monitoring_frequency": decision.monitoring_frequency
            },
            "recommendations": decision.recommendations,
            "rationale": decision.decision_rationale,
            "next_review_date": decision.next_review_date.isoformat() if decision.next_review_date else None
        }
    
    def export_decisions(self, decisions: List[PolicyDecision], format: str = "json") -> str:
        """
        Export decisions in various formats
        
        Args:
            decisions: List of PolicyDecision objects
            format: Export format (json, csv, summary)
            
        Returns:
            Formatted string
        """
        if format == "json":
            return json.dumps([self.generate_review_document(d) for d in decisions], indent=2)
        elif format == "summary":
            summary = []
            summary.append("COI Policy Decision Summary")
            summary.append("=" * 50)
            
            # Statistics
            total = len(decisions)
            critical = sum(1 for d in decisions if d.risk_level == RiskLevel.CRITICAL)
            high = sum(1 for d in decisions if d.risk_level == RiskLevel.HIGH)
            moderate = sum(1 for d in decisions if d.risk_level == RiskLevel.MODERATE)
            low = sum(1 for d in decisions if d.risk_level == RiskLevel.LOW)
            
            summary.append(f"Total Disclosures Evaluated: {total}")
            summary.append(f"Critical Risk: {critical} ({critical/total*100:.1f}%)")
            summary.append(f"High Risk: {high} ({high/total*100:.1f}%)")
            summary.append(f"Moderate Risk: {moderate} ({moderate/total*100:.1f}%)")
            summary.append(f"Low Risk: {low} ({low/total*100:.1f}%)")
            
            # Insurmountable conflicts
            insurmountable = sum(1 for d in decisions if d.conflict_type == ConflictType.INSURMOUNTABLE)
            if insurmountable > 0:
                summary.append(f"\nInsurmountable Conflicts: {insurmountable}")
                for d in decisions:
                    if d.conflict_type == ConflictType.INSURMOUNTABLE:
                        summary.append(f"  - {d.provider_name}: {', '.join(d.policy_violations)}")
            
            # Management plans required
            mgmt_plans = sum(1 for d in decisions if d.management_plan_required)
            summary.append(f"\nManagement Plans Required: {mgmt_plans}")
            
            # Recusals required
            recusals = sum(1 for d in decisions if d.recusal_required)
            summary.append(f"Recusals Required: {recusals}")
            
            return "\n".join(summary)
        
        return ""