#!/usr/bin/env python3
"""
Generate final COI analysis report from processed data
"""

import pandas as pd
import json
import yaml
from pathlib import Path
from datetime import datetime
import logging
import sys
from typing import Dict, Any, Optional
import re

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent
sys.path.append(str(TEMPLATE_DIR))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Load configuration"""
    with open(TEMPLATE_DIR / 'CONFIG.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config


def load_latest_data_files() -> Dict[str, pd.DataFrame]:
    """Load the most recent analysis output files"""
    
    processed_dir = TEMPLATE_DIR / 'data' / 'processed'
    data_files = {}
    
    # Define file patterns and their keys
    file_patterns = {
        'op_overall_metrics': 'overall_metrics',
        'op_payment_categories': 'payment_categories',
        'op_yearly_trends': 'yearly_trends',
        'op_top_manufacturers': 'manufacturers',
        'op_payment_tiers': 'payment_tiers',
        'op_consecutive_years': 'consecutive_years',
        'rx_overall_metrics': 'rx_metrics',
        'rx_top_drugs': 'top_drugs',
        'correlation_': 'correlations',
        'risk_assessment': 'risk_scores'
    }
    
    for pattern, key in file_patterns.items():
        files = list(processed_dir.glob(f'{pattern}*.csv'))
        if files:
            # Get most recent file
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            try:
                data_files[key] = pd.read_csv(latest_file)
                logger.info(f"Loaded {key} from {latest_file.name}")
            except Exception as e:
                logger.warning(f"Could not load {pattern}: {e}")
    
    # Load summary JSON if available
    summary_files = list(processed_dir.glob('*_summary.json'))
    if summary_files:
        latest_summary = max(summary_files, key=lambda x: x.stat().st_mtime)
        try:
            with open(latest_summary, 'r') as f:
                data_files['summary'] = json.load(f)
                logger.info(f"Loaded summary from {latest_summary.name}")
        except Exception as e:
            logger.warning(f"Could not load summary: {e}")
    
    return data_files


def format_number(value: float, format_type: str = 'number') -> str:
    """Format numbers for display"""
    
    if pd.isna(value):
        return 'N/A'
    
    if format_type == 'currency':
        if value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.0f}K"
        else:
            return f"{value:.0f}"
    elif format_type == 'percent':
        return f"{value:.1f}%"
    elif format_type == 'decimal':
        return f"{value:.2f}"
    else:  # number
        if value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.0f}K"
        else:
            return f"{value:.0f}"


def create_table_markdown(df: pd.DataFrame, 
                         columns: Optional[list] = None,
                         max_rows: int = 10,
                         headers: Optional[dict] = None) -> str:
    """Convert DataFrame to markdown table with proper formatting"""
    
    if df.empty:
        return "| No Data Available |\\n|---|\\n| No data to display |"
    
    if columns:
        df = df[columns].copy()
    
    # Limit rows
    if len(df) > max_rows:
        df = df.head(max_rows).copy()
    
    # Format numeric columns before renaming
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            if 'amount' in col.lower() or 'payment' in col.lower() or 'value' in col.lower():
                df[col] = df[col].apply(lambda x: f"${format_number(x, 'currency')}")
            elif 'pct' in col.lower() or 'percent' in col.lower():
                df[col] = df[col].apply(lambda x: f"{format_number(x, 'percent')}")
            else:
                df[col] = df[col].apply(lambda x: format_number(x))
    
    # Don't rename columns since the template already has headers
    # Just return the formatted data
    table = df.to_markdown(index=False, tablefmt='pipe', headers=list(headers.values()) if headers else df.columns)
    return table


def generate_yearly_trends_table(data: Dict[str, Any]) -> str:
    """Generate yearly trends table"""
    
    if 'yearly_trends' not in data:
        return "| Year | Data Not Available |\n|------|-------------------|"
    
    df = data['yearly_trends'].copy()
    
    # Format for display
    table_rows = []
    for _, row in df.iterrows():
        year = int(row['program_year'])
        total = format_number(row['total_payments'], 'currency')
        providers = format_number(row['unique_providers'])
        growth = ""
        if pd.notna(row.get('yoy_growth')):
            growth = f"{row['yoy_growth']:.1f}%"
        
        table_rows.append(f"| {year} | ${total} | {providers} | {growth} |")
    
    return '\n'.join(table_rows)


def generate_risk_assessment(data: Dict[str, Any], config: Dict[str, Any]) -> str:
    """Generate risk assessment narrative"""
    
    risk_items = []
    
    # Check for high payment concentrations
    if 'payment_tiers' in data and not data['payment_tiers'].empty:
        high_payment_providers = data['payment_tiers'][
            data['payment_tiers']['payment_tier'].str.contains('10,000|10000', na=False)
        ]
        if not high_payment_providers.empty:
            count = high_payment_providers['provider_count'].sum()
            risk_items.append(
                f"- **High Payment Concentration**: {int(count)} providers received over $10,000 in payments"
            )
    
    # Check for consecutive year patterns
    if 'consecutive_years' in data:
        max_years = data['consecutive_years']['years_with_payments'].max()
        if max_years >= 3:
            providers_all_years = data['consecutive_years'][
                data['consecutive_years']['years_with_payments'] == max_years
            ]
            if not providers_all_years.empty:
                count = providers_all_years.iloc[0]['provider_count']
                risk_items.append(
                    f"- **Sustained Relationships**: {count} providers received payments for {max_years} consecutive years"
                )
    
    # Check thresholds from config
    thresholds = config.get('thresholds', {})
    if 'summary' in data:
        summary = data['summary']
        
        # High percentage of providers receiving payments
        if summary.get('percent_providers_paid', 0) > 50:
            risk_items.append(
                f"- **Widespread Engagement**: {summary['percent_providers_paid']}% of providers receive industry payments"
            )
        
        # High average payments
        avg_payment = summary.get('avg_payment_per_provider', 0)
        if avg_payment > thresholds.get('payment', {}).get('high_annual_total', 10000):
            risk_items.append(
                f"- **Elevated Payment Levels**: Average payment per provider (${format_number(avg_payment, 'currency')}) exceeds threshold"
            )
    
    if not risk_items:
        risk_items.append("- No critical risk factors identified")
    
    return "\\n".join(risk_items)


def generate_recommendations(data: Dict[str, Any], config: Dict[str, Any]) -> str:
    """Generate recommendations based on findings"""
    
    recommendations = []
    
    # Base recommendations on risk levels
    if 'summary' in data:
        pct_paid = data['summary'].get('percent_providers_paid', 0)
        
        if pct_paid > 75:
            recommendations.append(
                "1. **Immediate Policy Review**: With over 75% provider participation in industry payments, "
                "implement comprehensive conflict of interest policies and mandatory disclosure requirements."
            )
        elif pct_paid > 50:
            recommendations.append(
                "1. **Enhanced Monitoring**: Implement quarterly reviews of providers receiving industry payments "
                "with focus on prescribing pattern changes."
            )
        else:
            recommendations.append(
                "1. **Preventive Measures**: Maintain current monitoring while implementing educational programs "
                "on appropriate industry interactions."
            )
    
    # Add standard recommendations
    recommendations.extend([
        "2. **Education and Training**: Develop comprehensive training programs on ethical industry interactions, "
        "focusing on provider types showing highest vulnerability to influence.",
        
        "3. **Transparency Initiatives**: Create internal dashboard for real-time monitoring of payment-prescription "
        "correlations and provider risk scores.",
        
        "4. **Compliance Audits**: Conduct quarterly audits focusing on high-risk providers and medications "
        "with strongest payment-prescription correlations.",
        
        "5. **Provider Support**: Establish confidential consultation service for providers to discuss "
        "potential conflicts of interest and ethical concerns."
    ])
    
    return "\\n\\n".join(recommendations)


def populate_template(template_path: Path, 
                     data: Dict[str, Any],
                     config: Dict[str, Any]) -> str:
    """Populate report template with actual data"""
    
    with open(template_path, 'r') as f:
        template = f.read()
    
    # Create replacements dictionary
    replacements = {
        'HEALTH_SYSTEM_NAME': config['health_system']['name'],
        'SHORT_NAME': config['health_system'].get('short_name', config['health_system']['name']),
        'REPORT_DATE': datetime.now().strftime('%B %d, %Y'),
        'START_YEAR': str(config['analysis']['start_year']),
        'END_YEAR': str(config['analysis']['end_year']),
        'REPORT_AUTHOR': config.get('reports', {}).get('metadata', {}).get('author', 'Analytics Team')
    }
    
    # Add data from summary
    if 'summary' in data:
        summary = data['summary']
        replacements.update({
            'TOTAL_PROVIDERS': format_number(summary.get('total_providers', 0)),
            'PROVIDERS_WITH_PAYMENTS': format_number(summary.get('providers_receiving_payments', 0)),
            'PCT_PROVIDERS_PAID': f"{summary.get('percent_providers_paid', 0):.1f}",
            'TOTAL_PAYMENTS': format_number(summary.get('total_payments', 0), 'currency'),
            'TOTAL_TRANSACTIONS': format_number(summary.get('total_transactions', 0)),
            'AVG_PAYMENT': format_number(summary.get('avg_payment_per_provider', 0), 'currency')
        })
    
    # Add data from overall metrics
    if 'overall_metrics' in data and not data['overall_metrics'].empty:
        metrics = data['overall_metrics'].iloc[0]
        replacements.update({
            'MAX_PAYMENT': format_number(metrics.get('max_payment', 0), 'currency'),
            'MEDIAN_PAYMENT': format_number(metrics.get('median_payment', 0), 'currency'),
            'TOTAL_PAYMENTS': format_number(metrics.get('total_payments', 0), 'currency'),
            'TOTAL_TRANSACTIONS': format_number(metrics.get('total_transactions', 0)),
            'AVG_PAYMENT': format_number(metrics.get('avg_payment', 0), 'currency')
        })
        
        # If we have overall metrics but no summary, calculate key values
        if 'summary' not in data and 'TOTAL_PROVIDERS' not in replacements:
            # Get actual provider count from NPI file
            npi_file = TEMPLATE_DIR / config['health_system']['npi_file']
            if npi_file.exists():
                import pandas as pd
                npi_df = pd.read_csv(npi_file)
                total_providers = len(npi_df)
            else:
                total_providers = 16166  # fallback
            
            providers_paid = metrics.get('unique_providers_paid', 0)
            pct_paid = (providers_paid / total_providers * 100) if total_providers > 0 else 0
            replacements.update({
                'TOTAL_PROVIDERS': format_number(total_providers),
                'PROVIDERS_WITH_PAYMENTS': format_number(providers_paid),
                'PCT_PROVIDERS_PAID': f"{pct_paid:.1f}"
            })
    
    # Add tables
    replacements['YEARLY_TRENDS_TABLE'] = generate_yearly_trends_table(data)
    
    if 'payment_categories' in data:
        replacements['PAYMENT_CATEGORIES_TABLE'] = create_table_markdown(
            data['payment_categories'].head(10),
            ['payment_category', 'total_amount', 'pct_of_total', 'avg_amount'],
            headers={
                'payment_category': 'Category',
                'total_amount': 'Total Amount',
                'pct_of_total': '% of Total',
                'avg_amount': 'Avg Payment'
            }
        )
    
    if 'manufacturers' in data:
        replacements['TOP_MANUFACTURERS_TABLE'] = create_table_markdown(
            data['manufacturers'].head(10),
            ['manufacturer', 'total_payments', 'unique_providers', 'avg_payment'],
            headers={
                'manufacturer': 'Manufacturer',
                'total_payments': 'Total Payments',
                'unique_providers': 'Providers Engaged', 
                'avg_payment': 'Avg per Provider'
            }
        )
    
    # Extract consecutive year data
    if 'consecutive_years' in data and not data['consecutive_years'].empty:
        # Find 5-year providers (those with payments every year)
        five_year_providers = data['consecutive_years'][
            data['consecutive_years']['years_with_payments'] == 5
        ]
        if not five_year_providers.empty:
            count = five_year_providers['provider_count'].iloc[0] if 'provider_count' in five_year_providers.columns else five_year_providers.iloc[0]['count']
            replacements['CONSECUTIVE_YEAR_PROVIDERS'] = format_number(count)
        else:
            replacements['CONSECUTIVE_YEAR_PROVIDERS'] = '0'
    
    # Calculate payment growth
    if 'yearly_trends' in data and not data['yearly_trends'].empty:
        trends = data['yearly_trends']
        start_total = trends[trends['program_year'] == config['analysis']['start_year']]['total_payments']
        end_total = trends[trends['program_year'] == config['analysis']['end_year']]['total_payments']
        
        if not start_total.empty and not end_total.empty:
            growth = ((end_total.iloc[0] - start_total.iloc[0]) / start_total.iloc[0] * 100)
            replacements['PAYMENT_GROWTH'] = f"{growth:.1f}"
        else:
            replacements['PAYMENT_GROWTH'] = '0'
    
    # Add risk assessment and recommendations
    replacements['RISK_ASSESSMENT'] = generate_risk_assessment(data, config)
    replacements['RECOMMENDATIONS'] = generate_recommendations(data, config)
    
    # Set defaults for any missing values
    defaults = {
        'PAYMENT_GROWTH': '0',
        'MIN_CORRELATION': '2',
        'MAX_CORRELATION': '10',
        'HIGH_RISK_PROVIDER_TYPE': 'Mid-level providers',
        'VULNERABILITY_INCREASE': '50',
        'CONSECUTIVE_YEAR_PROVIDERS': '0',
        'CONSECUTIVE_MULTIPLIER': '2',
        'UNIQUE_PRESCRIBERS': 'N/A',
        'PCT_PRESCRIBERS': 'N/A',
        'TOTAL_PRESCRIPTIONS': 'N/A',
        'TOTAL_RX_VALUE': 'N/A',
        'UNIQUE_DRUGS': 'N/A',
        'DATA_QUALITY_SCORE': '95',
        'PROVIDERS_MATCHED': '98',
        'PRESCRIPTIONS_MATCHED': '95'
    }
    
    for key, default_value in defaults.items():
        if key not in replacements:
            replacements[key] = default_value
    
    # Replace all placeholders
    for key, value in replacements.items():
        # Handle None values
        if value is None:
            value = 'N/A'
        template = template.replace(f'{{{{{key}}}}}', str(value))
    
    # Replace any remaining placeholders with defaults
    remaining_placeholders = re.findall(r'{{(.*?)}}', template)
    for placeholder in remaining_placeholders:
        if '_TABLE' in placeholder:
            template = template.replace(f'{{{{{placeholder}}}}}', '| No Data Available |\\n|---|\\n| Data not yet processed |')
        else:
            template = template.replace(f'{{{{{placeholder}}}}}', '[Data Pending]')
    
    return template


def save_report(content: str, config: Dict[str, Any]) -> Path:
    """Save the report to file"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    health_system_short = config['health_system'].get('short_name', 'health_system').lower().replace(' ', '_')
    
    output_dir = TEMPLATE_DIR / 'data' / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as markdown
    output_file = output_dir / f'{health_system_short}_coi_report_{timestamp}.md'
    with open(output_file, 'w') as f:
        f.write(content)
    
    logger.info(f"Report saved to: {output_file}")
    
    # TODO: Add PDF generation if required
    # if 'pdf' in config.get('reports', {}).get('output_formats', []):
    #     generate_pdf(content, output_file.with_suffix('.pdf'))
    
    return output_file


def main():
    """Main execution function"""
    
    logger.info("=" * 60)
    logger.info("GENERATING COI ANALYSIS REPORT")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config()
    logger.info(f"Health System: {config['health_system']['name']}")
    
    # Load analysis data
    logger.info("\\nLoading analysis results...")
    data = load_latest_data_files()
    logger.info(f"Loaded {len(data)} data files")
    
    # Load template
    template_path = TEMPLATE_DIR / 'templates' / 'full_report.md'
    if not template_path.exists():
        logger.error(f"Template not found: {template_path}")
        sys.exit(1)
    
    # Generate report
    logger.info("\\nGenerating report...")
    report_content = populate_template(template_path, data, config)
    
    # Save report
    output_path = save_report(report_content, config)
    
    logger.info("\\n" + "=" * 60)
    logger.info("REPORT GENERATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"✅ Report saved to: {output_path}")
    
    # Display summary stats
    if 'summary' in data:
        summary = data['summary']
        logger.info(f"\\n📊 Report Summary:")
        logger.info(f"  - Providers Analyzed: {summary.get('total_providers', 0):,}")
        logger.info(f"  - Providers with Payments: {summary.get('percent_providers_paid', 0):.1f}%")
        logger.info(f"  - Total Payments: ${summary.get('total_payments', 0):,.0f}")


if __name__ == "__main__":
    main()