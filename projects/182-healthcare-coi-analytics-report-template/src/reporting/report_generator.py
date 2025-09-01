"""
Unified Report Generator Module
Generates professional reports in multiple formats using LLM analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import yaml
from jinja2 import Environment, FileSystemLoader
from .llm_client import ClaudeLLMClient
from .data_mapper import SectionDataMapper

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Unified report generation for all report styles"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize report generator
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.template_dir = Path("src/reporting/templates")
        self.output_dir = Path("reports")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=False
        )
        
        self.report_data = {}
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration file"""
        config_file = Path(config_path)
        if not config_file.exists():
            config_file = Path("CONFIG.yaml")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def generate_report(
        self,
        analysis_results: Dict[str, Any],
        report_style: str = "investigative",
        output_format: str = "markdown"
    ) -> str:
        """
        Generate report in specified style and format
        
        Args:
            analysis_results: Dictionary containing all analysis results
            report_style: Style of report (investigative, compliance, executive)
            output_format: Output format (markdown, html, pdf)
            
        Returns:
            Path to generated report
        """
        logger.info(f"Generating {report_style} report in {output_format} format")
        
        # Prepare report data
        self.report_data = self._prepare_report_data(analysis_results)
        
        # Generate report based on style
        if report_style == "investigative":
            content = self._generate_investigative_report()
        elif report_style == "compliance":
            content = self._generate_compliance_report()
        elif report_style == "executive":
            content = self._generate_executive_report()
        else:
            raise ValueError(f"Unknown report style: {report_style}")
        
        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.config['health_system']['short_name'].lower()}_{report_style}_report_{timestamp}"
        
        if output_format == "markdown":
            output_path = self.output_dir / f"{filename}.md"
            with open(output_path, 'w') as f:
                f.write(content)
        elif output_format == "html":
            output_path = self.output_dir / f"{filename}.html"
            html_content = self._convert_to_html(content)
            with open(output_path, 'w') as f:
                f.write(html_content)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        logger.info(f"Report saved to: {output_path}")
        return str(output_path)
    
    def _prepare_report_data(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare and format data for report generation"""
        data = {
            'config': self.config,
            'generated_at': datetime.now().strftime("%B %d, %Y"),
            'health_system': self.config['health_system']['name'],
            'short_name': self.config['health_system']['short_name']
        }
        
        # Format Open Payments data
        if 'open_payments' in analysis_results:
            op = analysis_results['open_payments']
            data['open_payments'] = {
                'metrics': self._format_metrics(op.get('overall_metrics', {})),
                'yearly_trends': op.get('yearly_trends', pd.DataFrame()),
                'payment_categories': op.get('payment_categories', pd.DataFrame()),
                'top_manufacturers': op.get('top_manufacturers', pd.DataFrame()),
                'distribution': op.get('payment_distribution', {})
            }
        
        # Format Prescription data
        if 'prescriptions' in analysis_results:
            rx = analysis_results['prescriptions']
            data['prescriptions'] = {
                'metrics': self._format_metrics(rx.get('overall_metrics', {})),
                'yearly_trends': rx.get('yearly_trends', pd.DataFrame()),
                'top_drugs': rx.get('top_drugs', pd.DataFrame()),
                'high_cost_drugs': rx.get('high_cost_drugs', pd.DataFrame())
            }
        
        # Format Correlation data
        if 'correlations' in analysis_results:
            corr = analysis_results['correlations']
            data['correlations'] = {
                'drug_specific': corr.get('drug_specific', pd.DataFrame()),
                'payment_tiers': corr.get('payment_tiers', pd.DataFrame()),
                'provider_vulnerability': corr.get('provider_type_vulnerability', pd.DataFrame()),
                'influence_metrics': corr.get('influence_metrics', {})
            }
        
        # Format Risk Assessment data
        if 'risk_assessment' in analysis_results:
            risk = analysis_results['risk_assessment']
            data['risk_assessment'] = {
                'summary': risk.get('summary', {}),
                'distribution': risk.get('distribution', []),
                'top_risks': risk.get('top_risks', [])
            }
        
        # Format Specialty Analysis data
        if 'specialty_analysis' in analysis_results:
            spec = analysis_results['specialty_analysis']
            data['specialty_analysis'] = {
                'overview': spec.get('specialty_overview', pd.DataFrame()),
                'correlations': spec.get('specialty_correlations', pd.DataFrame()),
                'vulnerability': spec.get('specialty_vulnerability', pd.DataFrame())
            }
        
        return data
    
    def _format_metrics(self, metrics: Dict[str, Any]) -> Dict[str, str]:
        """Format metrics for display"""
        formatted = {}
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                if 'payment' in key or 'cost' in key or 'value' in key:
                    formatted[key] = f"${value:,.0f}"
                elif 'percent' in key or 'pct' in key:
                    formatted[key] = f"{value:.1f}%"
                else:
                    formatted[key] = f"{value:,.0f}"
            else:
                formatted[key] = str(value)
        return formatted
    
    def _generate_investigative_report(self) -> str:
        """Generate investigative journalism style report using LLM analysis"""
        
        # Load section prompts
        prompts_path = Path(__file__).parent / 'section_prompts.yaml'
        with open(prompts_path, 'r') as f:
            prompts_config = yaml.safe_load(f)
        
        # Initialize LLM client and data mapper
        llm_client = ClaudeLLMClient()
        data_mapper = SectionDataMapper(self.report_data)
        
        # Process sections in order
        generated_sections = {}
        section_order = prompts_config.get('section_order', [])
        
        for section_name in section_order:
            if section_name == 'executive_summary':
                # Skip executive summary for now, generate it last
                continue
            
            logger.info(f"Generating section: {section_name}")
            
            # Get section configuration
            section_config = prompts_config.get(section_name, {})
            
            # Get required data for this section
            required_data = section_config.get('data_required', [])
            section_data = data_mapper.get_section_data(section_name, required_data)
            
            # Generate narrative for this section
            try:
                narrative = llm_client.generate_section(
                    section_config,
                    section_data,
                    previous_sections=generated_sections
                )
                generated_sections[section_name] = narrative
            except Exception as e:
                logger.error(f"Failed to generate {section_name}: {e}")
                # Fallback to template-based generation
                generated_sections[section_name] = self._generate_fallback_section(section_name)
        
        # Generate executive summary last with all sections complete
        logger.info("Generating executive summary")
        exec_config = prompts_config.get('executive_summary', {})
        exec_data = data_mapper.get_section_data('executive_summary', exec_config.get('data_required', []))
        
        # Add all section summaries to exec data
        exec_data['all_section_summaries'] = generated_sections
        
        try:
            executive_summary = llm_client.generate_section(
                exec_config,
                exec_data,
                previous_sections=generated_sections
            )
        except Exception as e:
            logger.error(f"Failed to generate executive summary: {e}")
            executive_summary = self._generate_fallback_executive_summary()
        
        # Assemble final report
        report_parts = [
            f"# {self.config['health_system']['name']} Conflict of Interest Analysis Report",
            f"\n*Generated: {datetime.now().strftime('%B %d, %Y')}*",
            f"*Analysis Period: {self.config['analysis']['start_year']}-{self.config['analysis']['end_year']}*\n",
            "---\n",
            "## Executive Summary\n",
            executive_summary,
            "\n---\n"
        ]
        
        # Add each section with proper formatting
        section_titles = {
            'payment_overview': '## 1. The Landscape of Industry Financial Relationships',
            'prescription_patterns': '## 2. Prescription Patterns',
            'correlation_analysis': '## 3. The Quantification of Influence',
            'provider_vulnerability': '## 4. The Hierarchy of Influence',
            'payment_tiers': '## 5. The Psychology of Micro-Influence',
            'consecutive_years': '## 6. The Compounding Effect of Sustained Relationships',
            'risk_assessment': '## 7. Risk Assessment',
            'recommendations': '## 8. Recommendations',
            'methodology_note': '## Appendix: Methodology'
        }
        
        for section_name in section_order:
            if section_name != 'executive_summary' and section_name in generated_sections:
                title = section_titles.get(section_name, f"## {section_name.replace('_', ' ').title()}")
                report_parts.append(f"{title}\n")
                report_parts.append(generated_sections[section_name])
                report_parts.append("\n---\n")
        
        return "\n".join(report_parts)
    
    def _generate_fallback_section(self, section_name: str) -> str:
        """Generate fallback content when LLM fails"""
        return f"[Section {section_name} - Data analysis in progress]"
    
    def _generate_fallback_executive_summary(self) -> str:
        """Generate fallback executive summary when LLM fails"""
        metrics = self.report_data.get('open_payments', {}).get('metrics', {})
        return f"""This analysis examines financial relationships between pharmaceutical/medical device 
        industries and {self.config['health_system']['name']} providers. 
        
        Key findings include {metrics.get('unique_providers', 0)} providers receiving 
        {metrics.get('total_payments', '$0')} in industry payments during the analysis period."""
    
    def _generate_compliance_report(self) -> str:
        """Generate compelling executive summary"""
        summary = f"""# {self.report_data['health_system']} Conflict of Interest Analysis Report

## Executive Summary

This comprehensive analysis examines the intricate financial relationships between the pharmaceutical and medical device industries and {self.report_data['health_system']}'s network of healthcare providers. The investigation reveals patterns that merit careful consideration regarding the nature and extent of industry influence on clinical decision-making.

"""
        
        if 'open_payments' in self.report_data:
            op = self.report_data['open_payments']['metrics']
            summary += f"""The scope of industry engagement is substantial and pervasive, with {op.get('unique_providers', 'N/A')} providers receiving ${op.get('total_payments', 'N/A')} in direct payments across {op.get('total_transactions', 'N/A')} transactions.

"""
        
        if 'correlations' in self.report_data:
            influence = self.report_data['correlations']['influence_metrics']
            if influence:
                summary += f"""### Key Observations

1. **Profound Correlation Patterns**: Providers receiving industry payments demonstrate prescribing volumes that exceed their unpaid colleagues by factors ranging from {influence.get('overall_rx_volume_influence', 1):.0f}x to {influence.get('overall_rx_cost_influence', 1):.0f}x.

2. **Extraordinary Return on Investment**: The analysis reveals that pharmaceutical manufacturers achieve returns exceeding ${influence.get('payment_efficiency', 0):.0f} per dollar invested in provider relationships.

"""
        
        return summary
    
    def _generate_open_payments_section_investigative(self) -> str:
        """Generate Open Payments section with narrative focus"""
        op = self.report_data['open_payments']
        
        section = """## The Landscape of Industry Financial Relationships

"""
        
        if op['metrics']:
            section += f"""The pharmaceutical and medical device industries have established extensive financial relationships with {self.report_data['health_system']} providers, creating a complex web of interactions that warrant careful examination.

### Overall Metrics
- **Unique Providers Receiving Payments**: {op['metrics'].get('unique_providers', 'N/A')}
- **Total Transactions**: {op['metrics'].get('total_transactions', 'N/A')}
- **Total Payments**: {op['metrics'].get('total_payments', 'N/A')}
- **Average Payment**: {op['metrics'].get('avg_payment', 'N/A')}
- **Maximum Single Payment**: {op['metrics'].get('max_payment', 'N/A')}

"""
        
        # Add yearly trends if available
        if not op['yearly_trends'].empty:
            section += """### Temporal Evolution of Financial Relationships

"""
            section += op['yearly_trends'].to_markdown(index=True)
            section += "\n\n"
        
        return section
    
    def _generate_correlations_section_investigative(self) -> str:
        """Generate correlations section with compelling narrative"""
        corr = self.report_data['correlations']
        
        section = """## The Quantification of Influence

Our analysis uncovers correlations between industry payments and prescribing patterns that challenge conventional understanding of marketing influence in healthcare.

"""
        
        # Drug-specific correlations
        if not corr['drug_specific'].empty:
            top_drugs = corr['drug_specific'].head(5)
            section += """### Extreme Influence Cases

"""
            for _, drug in top_drugs.iterrows():
                if drug['influence_factor'] > 10:
                    section += f"""**{drug['drug']}**
- Providers WITH payments: ${drug['avg_rx_value_with_payments']:,.0f} average prescription value
- Providers WITHOUT payments: ${drug['avg_rx_value_without_payments']:,.0f} average prescription value
- **Influence Factor: {drug['influence_factor']:.0f}x increased prescribing**

"""
        
        return section
    
    def _generate_vulnerability_section_investigative(self) -> str:
        """Generate provider vulnerability section"""
        if 'correlations' not in self.report_data:
            return ""
        
        vuln = self.report_data['correlations'].get('provider_vulnerability', pd.DataFrame())
        
        if vuln.empty:
            return ""
        
        section = """## The Hierarchy of Influence

Our analysis reveals that susceptibility to payment influence varies significantly across provider types, with mid-level providers demonstrating heightened vulnerability.

"""
        
        for _, row in vuln.iterrows():
            if row['rx_cost_influence_pct'] > 100:
                section += f"""**{row['provider_type']}**
- With payments: ${row['avg_rx_with_payments']:,.0f} average prescription value
- Without payments: ${row['avg_rx_without_payments']:,.0f} average prescription value
- **Influence Impact: {row['rx_cost_influence_pct']:.1f}% increase with payments**

"""
        
        return section
    
    def _generate_risk_section_investigative(self) -> str:
        """Generate risk assessment section"""
        if 'risk_assessment' not in self.report_data:
            return ""
        
        risk = self.report_data['risk_assessment']
        
        section = """## Risk Assessment

"""
        
        if risk['summary']:
            section += f"""### High-Risk Indicators
- **High-Risk Providers**: {risk['summary'].get('high_risk_count', 0):,}
- **Anomalies Detected**: {risk['summary'].get('anomaly_count', 0):,}
- **Mean Risk Score**: {risk['summary'].get('mean_risk_score', 0):.1f}/100

"""
        
        return section
    
    def _generate_recommendations_investigative(self) -> str:
        """Generate actionable recommendations"""
        return """## Recommendations

### Immediate Actions (0-3 months)
1. **Enhanced Monitoring Program**
   - Implement real-time dashboard tracking payment-prescription correlations
   - Flag providers exceeding influence thresholds
   - Weekly review of providers receiving >$5,000 in rolling 12-month period

2. **Mid-Level Provider Oversight**
   - Mandatory review of PA/NP prescribing patterns by supervising physicians
   - Enhanced supervision for providers receiving any industry payments
   - Quarterly audits of high-risk medication prescribing

### Short-term Interventions (3-12 months)
1. **Education and Training Initiative**
   - Mandatory conflict of interest training for all providers
   - Special focus sessions for mid-level providers on influence awareness
   - Case studies using actual institutional data (anonymized)

2. **Transparency Program**
   - Public reporting of provider payment profiles on institutional website
   - Patient notification when prescribed medications from paying manufacturers
   - Quarterly town halls on industry relationships

### Long-term Strategies (12+ months)
1. **Structural Reforms**
   - Transition to centralized, industry-independent CME funding
   - Establish institutional fund for conference attendance
   - Create internal product evaluation service independent of industry

2. **Continuous Improvement**
   - Quarterly analysis of payment-prescription correlations
   - Annual third-party audit of influence patterns
   - Benchmarking against peer institutions
"""
    
    def _generate_compliance_report(self) -> str:
        """Generate compliance-focused report"""
        # Simplified compliance report structure
        sections = []
        
        sections.append(f"# {self.report_data['health_system']} Compliance Risk Assessment\n")
        sections.append(f"Generated: {self.report_data['generated_at']}\n")
        
        # Add compliance-specific sections
        sections.append("## Regulatory Compliance Overview\n")
        sections.append("## Anti-Kickback Statute Risk Assessment\n")
        sections.append("## False Claims Act Exposure\n")
        sections.append("## Stark Law Considerations\n")
        sections.append("## Recommended Compliance Actions\n")
        
        return "\n".join(sections)
    
    def _generate_executive_report(self) -> str:
        """Generate executive briefing report"""
        # Simplified executive report structure
        sections = []
        
        sections.append(f"# Executive Briefing: {self.report_data['health_system']}\n")
        sections.append(f"Date: {self.report_data['generated_at']}\n")
        
        # Add executive-specific sections
        sections.append("## Key Metrics Dashboard\n")
        sections.append("## Strategic Risks\n")
        sections.append("## Financial Impact\n")
        sections.append("## Recommended Board Actions\n")
        
        return "\n".join(sections)
    
    def _convert_to_html(self, markdown_content: str) -> str:
        """Convert markdown to HTML with styling"""
        import markdown
        
        html_template = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Healthcare COI Analysis Report</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        h1 { color: #0c343a; border-bottom: 3px solid #eab96d; padding-bottom: 10px; }
        h2 { color: #0c343a; margin-top: 30px; }
        h3 { color: #4c94ed; }
        table {
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #0c343a;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        strong {
            color: #0c343a;
        }
    </style>
</head>
<body>
    {content}
</body>
</html>"""
        
        html_content = markdown.markdown(
            markdown_content,
            extensions=['extra', 'tables', 'toc']
        )
        
        return html_template.format(content=html_content)