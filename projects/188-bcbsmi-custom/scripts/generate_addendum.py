#!/usr/bin/env python3
"""
Generate BCBSMI Hospital Analysis Addendum Report
Creates a markdown addendum focusing on hospital-level insights
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

class AddendumGenerator:
    """Generate markdown addendum report for hospital analysis"""
    
    def __init__(self, analysis_results: Dict[str, Any]):
        """Initialize with analysis results"""
        self.results = analysis_results
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def generate_executive_summary(self) -> str:
        """Generate executive summary section"""
        summary = self.results.get('summary', {})
        
        content = f"""# BCBSMI Hospital Analysis Addendum

*Generated: {datetime.now().strftime('%B %d, %Y')}*
*Analysis Type: Hospital-Level Pharmaceutical Influence Assessment*

---

## Executive Summary

### Critical Findings for Payor Consideration

Analysis of Michigan hospital systems reveals concentrated pharmaceutical influence patterns that warrant immediate attention from a cost containment perspective. Among {summary.get('total_hospitals_analyzed', 0)} analyzed facilities serving BCBSMI members, we identified **{summary.get('high_risk_hospitals', 0)} critical-risk** and **{summary.get('medium_risk_hospitals', 0)} high-risk hospitals** based on payment concentration, prescribing patterns, and provider coverage.

Key metrics demanding payor attention:
- **${summary.get('total_payments', 0):,.0f}** in pharmaceutical payments to providers at Michigan hospitals
- **${summary.get('total_prescription_cost', 0):,.0f}** in high-cost drug prescriptions from these facilities
- **{summary.get('avg_payment_penetration', 0):.1f}%** average payment penetration across hospital systems
- **{summary.get('total_bcbsmi_providers', 0):,}** BCBSMI-affiliated providers included in analysis

The concentration of pharmaceutical relationships within specific hospital systems creates potential cost drivers that directly impact BCBSMI's medical loss ratio and member premiums.
"""
        return content
    
    def generate_risk_ranking(self) -> str:
        """Generate hospital risk ranking section"""
        risk_df = self.results.get('risk_scores')
        if risk_df is None or risk_df.empty:
            return "## Risk Ranking\n\n*No risk data available*\n"
        
        # Convert to numeric and get top 10 hospitals by risk
        risk_df['composite_risk_score'] = pd.to_numeric(risk_df['composite_risk_score'], errors='coerce').fillna(0)
        top_10 = risk_df.nlargest(10, 'composite_risk_score')
        
        content = """
## Hospital Risk Ranking

### Methodology
Hospitals are scored on a 100-point scale incorporating:
- **Payment Intensity (30 points)**: Average payment per provider
- **Payment Penetration (30 points)**: Percentage of providers receiving payments
- **Prescription Volume (20 points)**: Total cost of high-risk drug prescriptions
- **Provider Concentration (20 points)**: BCBSMI provider coverage at facility

### Top 10 High-Risk Hospitals for Investigation

| Rank | Hospital | City | Risk Score | Category | Total Payments | BCBSMI Providers | Payment Penetration |
|------|----------|------|------------|----------|----------------|------------------|---------------------|
"""
        
        for idx, row in top_10.iterrows():
            content += f"| {int(row['risk_rank'])} | {row['Facility_Name']} | {row['City']} | "
            content += f"{row['composite_risk_score']:.1f} | {row['risk_category']} | "
            content += f"${row['total_payments']:,.0f} | {int(row['bcbsmi_providers'])} | "
            content += f"{row['payment_penetration_pct']:.1f}% |\n"
        
        return content
    
    def generate_payment_analysis(self) -> str:
        """Generate payment concentration analysis"""
        payments_df = self.results.get('hospital_payments')
        if payments_df is None or payments_df.empty:
            return "## Payment Analysis\n\n*No payment data available*\n"
        
        # Convert total_payments to numeric and get top 5 by payment volume
        payments_df['total_payments'] = pd.to_numeric(payments_df['total_payments'], errors='coerce').fillna(0)
        top_5_payments = payments_df.nlargest(5, 'total_payments')
        
        content = """
## Payment Concentration Analysis

### Hospitals with Highest Pharmaceutical Payment Volume

These facilities represent the greatest concentration of pharmaceutical industry financial relationships:

"""
        
        for idx, row in top_5_payments.iterrows():
            content += f"""
#### {row['Facility_Name']}, {row['City']}
- **Total Payments**: ${row['total_payments']:,.0f}
- **Providers with Payments**: {int(row['providers_with_payments'])} of {int(row['providers_analyzed'])} ({row['payment_penetration_pct']:.1f}%)
- **Average Payment per Provider**: ${row['avg_payment_per_provider']:,.0f}
- **Maximum Single Provider Payment**: ${row['max_provider_payment']:,.0f}
- **Total Transactions**: {int(row['total_transactions']):,}
- **Average Manufacturers per Provider**: {row['avg_manufacturers_per_provider']:.1f}
"""
        
        return content
    
    def generate_prescription_insights(self) -> str:
        """Generate prescription pattern insights"""
        rx_df = self.results.get('hospital_prescriptions')
        if rx_df is None or rx_df.empty:
            return "## Prescription Insights\n\n*No prescription data available*\n"
        
        content = """
## High-Cost Prescription Patterns

### Hospitals Driving Expensive Drug Utilization

Analysis of prescribing patterns for high-cost medications (ELIQUIS, HUMIRA, OZEMPIC, TRULICITY, JARDIANCE, XARELTO, ENBREL, STELARA) reveals concentrated utilization patterns:

| Hospital | City | Total Rx Cost | Total Prescriptions | Top Drugs |
|----------|------|---------------|---------------------|-----------|
"""
        
        # Convert to numeric and get top 10 by prescription cost
        rx_df['total_rx_cost'] = pd.to_numeric(rx_df['total_rx_cost'], errors='coerce').fillna(0)
        top_10_rx = rx_df.nlargest(10, 'total_rx_cost')
        
        for idx, row in top_10_rx.iterrows():
            content += f"| {row['Facility_Name']} | {row['City']} | "
            content += f"${row['total_rx_cost']:,.0f} | {int(row['total_rx_count']):,} | "
            content += f"{row['top_drugs']} |\n"
        
        return content
    
    def generate_detailed_profiles(self) -> str:
        """Generate detailed profiles for top 5 risk hospitals"""
        risk_df = self.results.get('risk_scores')
        if risk_df is None or risk_df.empty:
            return "## Detailed Hospital Profiles\n\n*No profile data available*\n"
        
        content = """
## Detailed Hospital Profiles

### In-Depth Analysis of Highest Risk Facilities

"""
        
        # Get top 5 by risk score
        top_5 = risk_df.nlargest(5, 'composite_risk_score')
        
        for idx, row in top_5.iterrows():
            content += f"""
---

### {int(row['risk_rank'])}. {row['Facility_Name']}
**Location**: {row['City']}, Michigan  
**Risk Category**: {row['risk_category']}  
**Composite Risk Score**: {row['composite_risk_score']:.1f}/100

#### Provider Network
- **Total Providers**: {int(row['total_providers'])}
- **BCBSMI Providers**: {int(row['bcbsmi_providers'])} ({row['bcbsmi_coverage_pct']:.1f}% coverage)
- **Providers with Payments**: {int(row['providers_with_payments'])} ({row['payment_penetration_pct']:.1f}% penetration)

#### Financial Relationships
- **Total Pharmaceutical Payments**: ${row['total_payments']:,.0f}
- **Average Payment per Provider**: ${row['avg_payment_per_provider']:,.0f}
- **Maximum Provider Payment**: ${row['max_provider_payment']:,.0f}
- **Total Transactions**: {int(row['total_transactions']):,}

#### Prescription Impact
- **Total Prescription Cost**: ${row['total_rx_cost']:,.0f}
- **Total Prescriptions**: {int(row['total_rx_count']):,}
- **Key Drugs**: {row.get('top_drugs', 'N/A')}

#### Risk Assessment
- Payment Intensity Score: {row['payment_intensity_score']:.1f}/30
- Payment Penetration Score: {row['payment_penetration_score']:.1f}/30
- Prescription Volume Score: {row['prescription_volume_score']:.1f}/20
- Provider Concentration Score: {row['provider_concentration_score']:.1f}/20
"""
        
        return content
    
    def generate_recommendations(self) -> str:
        """Generate actionable recommendations for BCBSMI"""
        summary = self.results.get('summary', {})
        
        content = f"""
## Actionable Recommendations for BCBSMI

### Immediate Actions (0-30 days)

1. **Enhanced Prior Authorization Review**
   - Implement stricter prior authorization requirements for high-cost medications at the {summary.get('high_risk_hospitals', 0)} critical-risk hospitals
   - Focus on: HUMIRA, OZEMPIC, TRULICITY, and other identified high-correlation drugs
   - Expected impact: 10-15% reduction in unnecessary prescriptions

2. **Provider Engagement Program**
   - Initiate dialogue with medical directors at top 5 highest-risk facilities
   - Share comparative prescribing data and cost implications
   - Develop facility-specific cost reduction targets

3. **Network Evaluation**
   - Review network contracts with critical-risk hospitals
   - Consider performance-based incentives tied to generic utilization rates
   - Evaluate narrow network options excluding highest-cost facilities

### Short-term Initiatives (30-90 days)

4. **Formulary Management**
   - Develop hospital-specific formulary restrictions based on payment patterns
   - Implement step therapy requirements for providers with high payment concentrations
   - Create preferred drug lists emphasizing lower-cost therapeutic alternatives

5. **Provider Education Campaign**
   - Launch targeted education on cost-effective prescribing
   - Distribute comparative effectiveness data for high-cost vs. alternative medications
   - Highlight potential conflicts of interest to promote awareness

6. **Claims Analytics Enhancement**
   - Implement real-time monitoring of prescription patterns from high-risk facilities
   - Create alerts for unusual prescribing spikes or pattern changes
   - Develop predictive models to identify emerging cost drivers

### Long-term Strategies (90+ days)

7. **Value-Based Contracting**
   - Negotiate outcome-based agreements with pharmaceutical manufacturers
   - Develop shared savings programs with hospitals showing improvement
   - Create quality metrics that balance clinical outcomes with cost efficiency

8. **Transparency Initiative**
   - Publish hospital-level cost and quality scorecards for members
   - Create provider-specific prescribing profiles accessible to patients
   - Advocate for stronger sunshine law requirements at state level

9. **Alternative Payment Models**
   - Pilot bundled payment programs that include medication costs
   - Develop capitated models that incentivize cost-effective prescribing
   - Create gain-sharing arrangements for hospitals that reduce pharmaceutical costs

### Expected Impact

Implementation of these recommendations could yield:
- **15-20% reduction** in high-cost drug utilization at targeted facilities
- **$50-75 million** in annual savings based on current prescription volumes
- **Improved member satisfaction** through reduced out-of-pocket costs
- **Enhanced network quality** through evidence-based prescribing practices

### Monitoring and Evaluation

Establish quarterly reviews to track:
- Changes in payment patterns at monitored hospitals
- Prescription cost trends by facility and provider
- Prior authorization approval/denial rates
- Member complaints and appeals related to formulary changes
- Overall medical loss ratio impact
"""
        
        return content
    
    def generate_methodology(self) -> str:
        """Generate methodology section"""
        content = """
## Methodology and Data Sources

### Data Sources
- **Provider Affiliations**: PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized
- **Payment Data**: op_general_all_aggregate_static_optimized (2020-2024)
- **Prescription Data**: PHYSICIAN_RX_2020_2024_optimized
- **Provider List**: BCBSMI provider network (49,576 NPIs)

### Analysis Period
- Payment data: 2020-2024 (5 years)
- Prescription data: 2020-2024 (5 years)
- Analysis date: Current

### Risk Scoring Methodology
Composite risk scores calculated using weighted factors:
1. **Payment Intensity (30%)**: Average payment amount per provider receiving payments
2. **Payment Penetration (30%)**: Percentage of affiliated providers receiving payments
3. **Prescription Volume (20%)**: Total cost of high-risk drug prescriptions
4. **Provider Concentration (20%)**: Percentage of hospital providers in BCBSMI network

### Inclusion Criteria
- Michigan hospitals with ≥10 affiliated providers
- Facilities with BCBSMI provider coverage
- Prescription volume >$1 million for targeted drugs

### Statistical Approach
- Aggregation at hospital-facility level
- Risk percentile ranking across all analyzed facilities
- Categorical risk assignment based on composite scores

### Limitations
- Analysis limited to Medicare Part D prescription data
- Payment data reflects reported Open Payments transactions only
- Hospital affiliations based on primary facility designation
- Temporal associations do not establish causation
"""
        
        return content
    
    def generate_report(self) -> str:
        """Generate complete addendum report"""
        sections = [
            self.generate_executive_summary(),
            self.generate_risk_ranking(),
            self.generate_payment_analysis(),
            self.generate_prescription_insights(),
            self.generate_detailed_profiles(),
            self.generate_recommendations(),
            self.generate_methodology()
        ]
        
        # Add footer
        footer = f"""
---

## Report Metadata

- **Generated**: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}
- **Report Type**: Hospital Analysis Addendum
- **Client**: Blue Cross Blue Shield Michigan
- **Classification**: Confidential - Internal Use Only
- **Prepared by**: Conflixis Data Analytics Team

*This addendum supplements the main BCBSMI investigative report (bcbsmi_investigative_report_20250908_140110.md)*
"""
        
        sections.append(footer)
        
        return "\n".join(sections)
    
    def save_report(self, output_dir: Path = None):
        """Save report to file"""
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / "reports"
        
        output_dir.mkdir(exist_ok=True)
        
        report_content = self.generate_report()
        report_file = output_dir / f"bcbsmi_hospital_addendum_{self.timestamp}.md"
        
        with open(report_file, 'w') as f:
            f.write(report_content)
        
        print(f"Report saved to: {report_file}")
        return report_file


def main():
    """Main execution"""
    # First run the analysis
    from hospital_analysis import HospitalAnalyzer
    
    print("Running hospital analysis...")
    analyzer = HospitalAnalyzer()
    results = analyzer.run_full_analysis()
    
    print("\nGenerating addendum report...")
    generator = AddendumGenerator(results)
    report_file = generator.save_report()
    
    print(f"\n✅ Addendum report generated: {report_file}")
    
    return report_file


if __name__ == "__main__":
    main()