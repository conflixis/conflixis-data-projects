"""
Data Mapper Module
Maps section data requirements to analysis results
"""

import pandas as pd
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class SectionDataMapper:
    """Maps section requirements to actual analysis data"""
    
    def __init__(self, report_data: Dict[str, Any]):
        """
        Initialize mapper with report data
        
        Args:
            report_data: Prepared report data from report generator
        """
        self.report_data = report_data
        self.data_map = self._build_data_map()
    
    def _build_data_map(self) -> Dict[str, Any]:
        """
        Build mapping of data keys to actual data locations
        
        Returns:
            Dictionary mapping data keys to values
        """
        data_map = {}
        
        # Open Payments mappings
        if 'open_payments' in self.report_data:
            op = self.report_data['open_payments']
            data_map['overall_payment_metrics'] = op.get('overall_metrics', {})
            data_map['overall_metrics'] = op.get('overall_metrics', {})
            data_map['yearly_trends'] = op.get('yearly_trends', pd.DataFrame())
            data_map['payment_categories'] = op.get('payment_categories', pd.DataFrame())
            data_map['top_manufacturers'] = op.get('top_manufacturers', pd.DataFrame())
            data_map['payment_distribution'] = op.get('payment_distribution', {})
            data_map['consecutive_years'] = op.get('consecutive_years', pd.DataFrame())
            data_map['consecutive_year_stats'] = self._calculate_consecutive_year_stats(
                op.get('consecutive_years', pd.DataFrame())
            )
        
        # Prescription mappings
        if 'prescriptions' in self.report_data:
            rx = self.report_data['prescriptions']
            data_map['prescription_metrics'] = rx.get('overall_metrics', {})
            data_map['top_drugs'] = rx.get('top_drugs', pd.DataFrame())
            data_map['high_cost_drugs'] = rx.get('high_cost_drugs', pd.DataFrame())
            data_map['specialty_patterns'] = rx.get('by_specialty', pd.DataFrame())
            data_map['provider_type_analysis'] = rx.get('by_provider_type', pd.DataFrame())
        
        # Correlation mappings
        if 'correlations' in self.report_data:
            corr = self.report_data['correlations']
            data_map['key_correlations'] = corr.get('drug_specific', pd.DataFrame())
            data_map['drug_correlations'] = corr.get('drug_specific', pd.DataFrame())
            data_map['influence_factors'] = self._extract_influence_factors(corr)
            data_map['roi_calculations'] = corr.get('roi_analysis', {})
            data_map['payment_tiers'] = corr.get('payment_tiers', pd.DataFrame())
            data_map['payment_tier_analysis'] = corr.get('payment_tiers', pd.DataFrame())
            data_map['small_payment_roi'] = self._calculate_small_payment_roi(corr)
            data_map['tier_prescribing_patterns'] = corr.get('payment_tiers', pd.DataFrame())
            # Note: report_generator stores this as 'provider_vulnerability', not 'provider_type_vulnerability'
            data_map['provider_vulnerability_summary'] = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', {}))
            data_map['provider_type_comparison'] = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', pd.DataFrame()))
            data_map['pa_influence_metrics'] = self._extract_pa_metrics(corr)
            data_map['np_influence_metrics'] = self._extract_np_metrics(corr)
            data_map['md_baseline'] = self._extract_md_baseline(corr)
            # Add full provider types data for comprehensive reporting
            data_map['all_provider_types'] = self._extract_all_provider_types(corr)
            data_map['specific_drug_examples'] = self._get_specific_drug_examples(corr)
        
        # Risk Assessment mappings
        if 'risk_assessment' in self.report_data:
            risk = self.report_data['risk_assessment']
            data_map['high_risk_indicators'] = risk.get('high_risk_indicators', [])
            data_map['compliance_scores'] = risk.get('compliance_scores', {})
            data_map['risk_distribution'] = risk.get('distribution', [])
            data_map['vulnerable_specialties'] = risk.get('vulnerable_specialties', [])
            data_map['key_findings_summary'] = risk.get('summary', {})
            data_map['highest_risk_areas'] = risk.get('top_risks', [])
            data_map['provider_categories'] = risk.get('provider_categories', {})
        
        # Consecutive years analysis - properly extract the data
        # Check in correlations first (where BigQuery analyzer puts it), then fall back to open_payments
        cy = None
        if 'correlations' in self.report_data and 'consecutive_years' in self.report_data['correlations']:
            cy = self.report_data['correlations']['consecutive_years']
        elif 'open_payments' in self.report_data and 'consecutive_years' in self.report_data['open_payments']:
            cy = self.report_data['open_payments']['consecutive_years']
        
        if cy is not None:
            if isinstance(cy, pd.DataFrame) and not cy.empty:
                # Create a formatted summary for the LLM
                # Handle both column name formats
                year_summary = {}
                for _, row in cy.iterrows():
                    # Get years value - check both possible column names
                    if 'years_of_payments' in row:
                        years_str = row['years_of_payments']
                        # Extract numeric value from strings like "1 Year", "2 Consecutive", etc.
                        if isinstance(years_str, str):
                            years = int(years_str.split()[0]) if years_str.split()[0].isdigit() else 1
                        else:
                            years = int(years_str)
                    else:
                        years = int(row.get('consecutive_years', 1))
                    
                    count = int(row.get('provider_count', 0))
                    avg_payment = float(row.get('avg_total_payments', row.get('avg_total_payment', 0)))
                    avg_rx = float(row.get('avg_total_rx_value', 0))
                    
                    year_summary[f"{years}_years"] = {
                        "provider_count": count,
                        "avg_total_payment": avg_payment,
                        "avg_rx_value": avg_rx,
                        "multiplier": row.get('multiplier_vs_single_year', 'N/A')
                    }
                
                data_map['consecutive_year_counts'] = year_summary
                data_map['consecutive_years_table'] = cy  # Store the full DataFrame for table rendering
                # Explicitly note the 5-year count
                five_year_mask = cy['years_of_payments'].str.contains('5') if 'years_of_payments' in cy.columns else cy.get('consecutive_years', pd.Series()) == 5
                five_year_providers = cy[five_year_mask]['provider_count'].iloc[0] if any(five_year_mask) and len(cy[five_year_mask]) > 0 else 0
                data_map['providers_all_5_years'] = int(five_year_providers)
                data_map['cumulative_prescribing'] = cy['avg_total_payments'].sum() if 'avg_total_payments' in cy.columns else cy.get('avg_total_payment', pd.Series()).sum()
                data_map['sustained_relationship_impact'] = self._calculate_sustained_impact(cy)
        else:
            # Provide empty data if not available
            data_map['consecutive_year_counts'] = {}
            data_map['providers_all_5_years'] = 0
        
        # Metadata
        data_map['data_sources'] = "CMS Open Payments Database, Medicare Part D Claims"
        data_map['analysis_period'] = "2020-2024"
        data_map['provider_count'] = data_map.get('overall_payment_metrics', {}).get('unique_providers', 0)
        
        # Data Lineage
        if 'data_lineage' in self.report_data:
            data_map['data_lineage_markdown'] = self.report_data['data_lineage'].get('markdown', 'Data lineage tracking is available.')
            data_map['data_lineage_summary'] = self.report_data['data_lineage'].get('summary', {})
        else:
            data_map['data_lineage_markdown'] = ''
        
        return data_map
    
    def get_section_data(self, section_name: str, required_keys: List[str]) -> Dict[str, Any]:
        """
        Get data required for a specific section
        
        Args:
            section_name: Name of the section
            required_keys: List of data keys required
            
        Returns:
            Dictionary with required data
        """
        section_data = {}
        
        for key in required_keys:
            if key in self.data_map:
                section_data[key] = self.data_map[key]
            else:
                logger.warning(f"Data key '{key}' not found for section '{section_name}'")
                section_data[key] = self._get_default_value(key)
        
        return section_data
    
    def _calculate_consecutive_year_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate statistics for consecutive year payments"""
        if df.empty:
            return {'providers_all_years': 0, 'average_total': 0}
        
        return {
            'providers_all_years': len(df),
            'average_total': df['total_amount'].mean() if 'total_amount' in df.columns else 0,
            'max_consecutive': 5  # Assuming 5-year period
        }
    
    def _extract_influence_factors(self, corr: Dict) -> Dict[str, float]:
        """Extract key influence factors from correlation data"""
        factors = {}
        
        if 'drug_specific' in corr and not corr['drug_specific'].empty:
            df = corr['drug_specific']
            if 'influence_factor' in df.columns:
                factors['max_influence'] = df['influence_factor'].max()
                factors['min_influence'] = df['influence_factor'].min()
                factors['avg_influence'] = df['influence_factor'].mean()
        
        return factors
    
    def _calculate_small_payment_roi(self, corr: Dict) -> float:
        """Calculate ROI for small payments"""
        if 'payment_tiers' in corr and not corr['payment_tiers'].empty:
            df = corr['payment_tiers']
            # Look for tier with payments < $100
            small_tier = df[df['tier'] == '<$100'] if 'tier' in df.columns else pd.DataFrame()
            if not small_tier.empty and 'roi' in small_tier.columns:
                return small_tier['roi'].iloc[0]
        
        return 0.0
    
    def _extract_pa_metrics(self, corr: Dict) -> Dict[str, Any]:
        """Extract PA-specific metrics"""
        # Check both possible keys (report_generator uses 'provider_vulnerability')
        df = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', pd.DataFrame()))
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Look for Physician Assistant in provider_type column
            pa_data = df[df['provider_type'] == 'Physician Assistant'] if 'provider_type' in df.columns else pd.DataFrame()
            if not pa_data.empty:
                return pa_data.iloc[0].to_dict()
        
        return {}  # Return empty dict if no data available
    
    def _extract_np_metrics(self, corr: Dict) -> Dict[str, Any]:
        """Extract NP-specific metrics"""
        # Check both possible keys (report_generator uses 'provider_vulnerability')
        df = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', pd.DataFrame()))
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Look for Nurse Practitioner in provider_type column
            np_data = df[df['provider_type'] == 'Nurse Practitioner'] if 'provider_type' in df.columns else pd.DataFrame()
            if not np_data.empty:
                return np_data.iloc[0].to_dict()
        
        return {}  # Return empty dict if no data available
    
    def _extract_md_baseline(self, corr: Dict) -> Dict[str, Any]:
        """Extract MD baseline metrics"""
        # Check both possible keys (report_generator uses 'provider_vulnerability')
        df = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', pd.DataFrame()))
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Look for Physician in provider_type column
            md_data = df[df['provider_type'] == 'Physician'] if 'provider_type' in df.columns else pd.DataFrame()
            if not md_data.empty:
                return md_data.iloc[0].to_dict()
        
        return {}  # Return empty dict if no data available
    
    def _extract_all_provider_types(self, corr: Dict) -> pd.DataFrame:
        """Extract all provider types from vulnerability analysis"""
        # Check both possible keys (report_generator uses 'provider_vulnerability')
        df = corr.get('provider_vulnerability', corr.get('provider_type_vulnerability', pd.DataFrame()))
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Return the full DataFrame with all provider types
            return df
        return pd.DataFrame()  # Return empty DataFrame if no data available
    
    def _get_specific_drug_examples(self, corr: Dict) -> List[Dict]:
        """Get specific drug examples with high influence factors"""
        examples = []
        
        if 'drug_specific' in corr and not corr['drug_specific'].empty:
            df = corr['drug_specific']
            # Get top 5 drugs by influence factor
            if 'influence_factor' in df.columns:
                top_drugs = df.nlargest(5, 'influence_factor')
                for _, row in top_drugs.iterrows():
                    examples.append({
                        'drug': row.get('drug_name', 'Unknown'),
                        'influence_factor': row.get('influence_factor', 0),
                        'roi': row.get('roi', 0)
                    })
        
        return examples  # Return empty list if no data available
    
    def _calculate_sustained_impact(self, cy_df: pd.DataFrame) -> float:
        """Calculate impact of sustained relationships"""
        if cy_df.empty:
            return 0.0
        
        # Compare providers with 5 years vs 1 year
        if 'years_with_payments' in cy_df.columns:
            five_year = cy_df[cy_df['years_with_payments'] == 5]
            one_year = cy_df[cy_df['years_with_payments'] == 1]
            
            if not five_year.empty and not one_year.empty:
                if 'total_prescriptions' in cy_df.columns:
                    five_year_avg = five_year['total_prescriptions'].mean()
                    one_year_avg = one_year['total_prescriptions'].mean()
                    if one_year_avg > 0:
                        return five_year_avg / one_year_avg
        
        return 0.0  # Return 0 if no data available
    
    def _get_default_value(self, key: str) -> Any:
        """Get default value for missing data key"""
        defaults = {
            'overall_payment_metrics': {'unique_providers': 0, 'total_payments': 0},
            'key_correlations': pd.DataFrame(),
            'drug_correlations': pd.DataFrame(),
            'influence_factors': {'max_influence': 0, 'min_influence': 0},
            'roi_calculations': {'average_roi': 0},
            'provider_vulnerability_summary': {'pa_increase': 0, 'np_increase': 0}
        }
        
        return defaults.get(key, {})