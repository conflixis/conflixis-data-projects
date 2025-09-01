#!/usr/bin/env python3
"""
Script to regenerate a specific section of an existing report
Useful for fixing individual sections without regenerating the entire report
"""

import sys
import re
import argparse
from pathlib import Path
import yaml

sys.path.append('.')

from src.data import DataLoader
from src.analysis import (
    OpenPaymentsAnalyzer,
    PrescriptionAnalyzer, 
    CorrelationAnalyzer,
    RiskScorer,
    SpecialtyAnalyzer
)
from src.reporting import ClaudeLLMClient, SectionDataMapper
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_sections_from_report(report_path: str) -> dict:
    """Extract existing sections from a report file"""
    with open(report_path, 'r') as f:
        content = f.read()
    
    sections = {}
    
    # Define section patterns and names
    section_patterns = {
        'payment_overview': r'## 1\. The Landscape of Industry Financial Relationships\n\n(.*?)(?=\n---\n|$)',
        'prescription_patterns': r'## 2\. Prescription Patterns.*?\n\n(.*?)(?=\n---\n|$)',
        'correlation_analysis': r'## 3\. The Quantification of Influence\n\n(.*?)(?=\n---\n|$)',
        'provider_vulnerability': r'## 4\. The Hierarchy of Influence\n\n(.*?)(?=\n---\n|$)',
        'payment_tiers': r'## 5\. The Psychology of Micro-Influence\n\n(.*?)(?=\n---\n|$)',
        'consecutive_years': r'## 6\. The Compounding Effect.*?\n\n(.*?)(?=\n---\n|$)',
        'risk_assessment': r'## 7\. Risk Assessment.*?\n\n(.*?)(?=\n---\n|$)',
        'recommendations': r'## 8\. Recommendations\n\n(.*?)(?=\n---\n|$)',
        'methodology_note': r'## Appendix: Methodology\n\n(.*?)(?=\n---\n|$)',
        'executive_summary': r'## Executive Summary\n\n(.*?)(?=\n---\n|$)'
    }
    
    for section_name, pattern in section_patterns.items():
        match = re.search(pattern, content, re.DOTALL)
        if match:
            sections[section_name] = match.group(1).strip()
            logger.info(f"Extracted section: {section_name} ({len(sections[section_name])} chars)")
    
    return sections


def run_full_analysis():
    """Run the complete analysis pipeline to get fresh data"""
    logger.info("Running full analysis pipeline...")
    
    # Load data
    loader = DataLoader()
    providers = loader.load_provider_npis()
    payments = loader.load_open_payments()
    prescriptions = loader.load_prescriptions()
    
    # Run analyses
    results = {}
    
    # Open Payments
    op_analyzer = OpenPaymentsAnalyzer(payments)
    results['open_payments'] = op_analyzer.analyze_all()
    
    # Prescriptions
    rx_analyzer = PrescriptionAnalyzer(prescriptions)
    results['prescriptions'] = rx_analyzer.analyze_all()
    
    # Correlations
    corr_analyzer = CorrelationAnalyzer(payments, prescriptions)
    results['correlations'] = corr_analyzer.analyze_all()
    
    # Risk Assessment
    config = loader.config
    risk_scorer = RiskScorer(config)
    risk_scores = risk_scorer.score_providers(payments, prescriptions)
    results['risk_assessment'] = risk_scorer.generate_risk_report()
    
    # Specialty Analysis
    spec_analyzer = SpecialtyAnalyzer(payments, prescriptions)
    results['specialty_analysis'] = spec_analyzer.analyze_all()
    
    return results


def regenerate_section(section_name: str, report_path: str, output_path: str = None):
    """Regenerate a specific section and update the report"""
    
    logger.info(f"Regenerating section: {section_name}")
    
    # Extract existing sections
    existing_sections = extract_sections_from_report(report_path)
    
    # Run analysis to get data
    analysis_results = run_full_analysis()
    
    # Load prompts
    with open('src/reporting/section_prompts.yaml', 'r') as f:
        prompts_config = yaml.safe_load(f)
    
    # Initialize LLM client and data mapper
    llm_client = ClaudeLLMClient()
    
    # Prepare report data (similar to ReportGenerator._prepare_report_data)
    report_data = {
        'open_payments': analysis_results['open_payments'],
        'prescriptions': analysis_results['prescriptions'],
        'correlations': analysis_results['correlations'],
        'risk_assessment': analysis_results['risk_assessment'],
        'specialty_analysis': analysis_results['specialty_analysis']
    }
    
    data_mapper = SectionDataMapper(report_data)
    
    # Get section configuration
    section_config = prompts_config.get(section_name)
    if not section_config:
        raise ValueError(f"Section '{section_name}' not found in prompts configuration")
    
    # Get required data for this section
    required_data = section_config.get('data_required', [])
    section_data = data_mapper.get_section_data(section_name, required_data)
    
    # For executive summary, we need all previous sections
    previous_sections = {}
    if section_name == 'executive_summary':
        previous_sections = {k: v for k, v in existing_sections.items() if k != 'executive_summary'}
        # Add section summaries to data
        section_data['all_section_summaries'] = previous_sections
    
    # Generate new section content
    logger.info(f"Generating new content for {section_name}...")
    try:
        new_content = llm_client.generate_section(
            section_config,
            section_data,
            previous_sections=previous_sections
        )
        logger.info(f"Generated {len(new_content)} characters for {section_name}")
    except Exception as e:
        logger.error(f"Failed to generate {section_name}: {e}")
        raise
    
    # Read the original report
    with open(report_path, 'r') as f:
        report_content = f.read()
    
    # Replace the section
    if section_name == 'executive_summary':
        # Executive summary is between "## Executive Summary\n\n" and "\n---\n"
        pattern = r'(## Executive Summary\n\n)(.*?)(\n---\n)'
        replacement = r'\1' + new_content + r'\3'
        updated_content = re.sub(pattern, replacement, report_content, flags=re.DOTALL)
    else:
        # For other sections, find and replace based on section number/title
        section_titles = {
            'payment_overview': '## 1. The Landscape of Industry Financial Relationships',
            'prescription_patterns': '## 2. Prescription Patterns',
            'correlation_analysis': '## 3. The Quantification of Influence',
            'provider_vulnerability': '## 4. The Hierarchy of Influence',
            'payment_tiers': '## 5. The Psychology of Micro-Influence',
            'consecutive_years': '## 6. The Compounding Effect',
            'risk_assessment': '## 7. Risk Assessment',
            'recommendations': '## 8. Recommendations',
            'methodology_note': '## Appendix: Methodology'
        }
        
        if section_name in section_titles:
            title = section_titles[section_name]
            pattern = f'({re.escape(title)}.*?\\n\\n)(.*?)(\\n---\\n)'
            replacement = r'\1' + new_content + r'\3'
            updated_content = re.sub(pattern, replacement, report_content, flags=re.DOTALL)
        else:
            raise ValueError(f"Don't know how to replace section: {section_name}")
    
    # Save the updated report
    if output_path:
        save_path = output_path
    else:
        save_path = report_path
    
    with open(save_path, 'w') as f:
        f.write(updated_content)
    
    logger.info(f"Updated report saved to: {save_path}")
    return new_content


def main():
    parser = argparse.ArgumentParser(description='Regenerate a specific section of a report')
    parser.add_argument('section', help='Section name to regenerate (e.g., executive_summary)')
    parser.add_argument('report', help='Path to the existing report file')
    parser.add_argument('--output', help='Output path (default: overwrite input file)')
    
    args = parser.parse_args()
    
    regenerate_section(args.section, args.report, args.output)


if __name__ == '__main__':
    main()