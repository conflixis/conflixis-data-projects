#!/usr/bin/env python3
"""
Standalone CLI for Healthcare COI Analytics
Works with existing processed data without BigQuery
"""

import click
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.data.mock_loader import MockDataLoader
from src.reporting.report_generator import ReportGenerator
from src.reporting.visualizations import VisualizationGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version='2.0.0')
def cli():
    """Healthcare COI Analytics Template - Professional conflict of interest analysis"""
    pass


@cli.command()
@click.option('--style', 
              type=click.Choice(['investigative', 'compliance', 'executive']),
              default='investigative',
              help='Report style')
@click.option('--format',
              type=click.Choice(['markdown', 'html']),
              default='markdown',
              help='Output format')
@click.option('--no-viz', is_flag=True, help='Skip visualization generation')
def analyze(style, format, no_viz):
    """Generate COI analysis report from processed data"""
    try:
        click.echo(click.style('🚀 Starting Healthcare COI Analysis', fg='green', bold=True))
        click.echo(f"Report style: {style}")
        click.echo(f"Output format: {format}")
        
        # Load data using mock loader
        click.echo("Loading processed data...")
        loader = MockDataLoader()
        analysis_results = loader.get_all_data()
        
        # Generate report
        click.echo("Generating report...")
        generator = ReportGenerator()
        report_path = generator.generate_report(
            analysis_results,
            report_style=style,
            output_format=format
        )
        
        # Generate visualizations if requested
        if not no_viz:
            click.echo("Generating visualizations...")
            viz_gen = VisualizationGenerator()
            viz_gen.generate_all_charts(analysis_results)
        
        # Display summary
        click.echo("\n" + "="*60)
        click.echo(click.style('✅ Analysis Complete!', fg='green', bold=True))
        click.echo("="*60)
        
        # Show key metrics
        op_metrics = analysis_results['open_payments']['overall_metrics']
        if op_metrics:
            click.echo(f"\n📊 Open Payments:")
            click.echo(f"   Providers: {op_metrics.get('unique_providers', 0):,}")
            click.echo(f"   Total: ${op_metrics.get('total_payments', 0):,.0f}")
        
        rx_metrics = analysis_results['prescriptions']['overall_metrics']
        if rx_metrics:
            click.echo(f"\n💊 Prescriptions:")
            click.echo(f"   Prescribers: {rx_metrics.get('unique_prescribers', 0):,}")
            click.echo(f"   Total Cost: ${rx_metrics.get('total_cost', 0):,.0f}")
        
        click.echo(f"\n📄 Report: {report_path}")
        
    except Exception as e:
        click.echo(click.style(f'❌ Analysis failed: {e}', fg='red'), err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@cli.command()
def info():
    """Display system information and data status"""
    click.echo(click.style('Healthcare COI Analytics Template v2.0', fg='cyan', bold=True))
    click.echo('='*60)
    
    # Check for processed data
    processed_dir = Path("data/processed")
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("*.csv"))
        click.echo(f"Processed data files: {len(csv_files)}")
        
        # Show most recent files
        if csv_files:
            recent = sorted(csv_files, key=lambda p: p.stat().st_mtime, reverse=True)[:5]
            click.echo("\nMost recent data files:")
            for f in recent:
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                click.echo(f"  - {f.name} ({mtime.strftime('%Y-%m-%d %H:%M')})")
    
    # Check configuration
    config_file = Path("CONFIG.yaml")
    if config_file.exists():
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        click.echo(f"\nHealth System: {config['health_system']['name']}")
        click.echo(f"Analysis Period: {config['analysis']['start_year']}-{config['analysis']['end_year']}")
    
    click.echo('='*60)


@cli.command()
def list_data():
    """List all available processed data files"""
    processed_dir = Path("data/processed")
    output_dir = Path("data/output")
    
    click.echo(click.style('Available Data Files', fg='cyan', bold=True))
    click.echo('='*60)
    
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("*.csv"))
        if csv_files:
            click.echo(f"\n📊 Processed Data ({len(csv_files)} files):")
            categories = {
                'op_': 'Open Payments',
                'rx_': 'Prescriptions',
                'correlation_': 'Correlations',
                'validation_': 'Validation'
            }
            
            for prefix, category in categories.items():
                matching = [f for f in csv_files if f.name.startswith(prefix)]
                if matching:
                    click.echo(f"\n{category}:")
                    for f in matching[:3]:  # Show first 3 of each type
                        size_kb = f.stat().st_size / 1024
                        click.echo(f"  - {f.name} ({size_kb:.1f} KB)")
    
    if output_dir.exists():
        reports = list(output_dir.glob("*.md"))
        if reports:
            click.echo(f"\n📄 Generated Reports ({len(reports)} files):")
            recent = sorted(reports, key=lambda p: p.stat().st_mtime, reverse=True)[:5]
            for r in recent:
                mtime = datetime.fromtimestamp(r.stat().st_mtime)
                click.echo(f"  - {r.name} ({mtime.strftime('%Y-%m-%d %H:%M')})")
    
    click.echo('='*60)


if __name__ == '__main__':
    cli()