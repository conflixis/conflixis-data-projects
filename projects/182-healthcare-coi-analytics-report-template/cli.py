#!/usr/bin/env python3
"""
Healthcare COI Analytics CLI
Command-line interface for running analyses and generating reports
"""

import click
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent))

from pipelines.full_analysis import FullAnalysisPipeline
from src.data import DataValidator
from src.reporting import VisualizationGenerator

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
@click.option('--config', default='config/config.yaml', help='Configuration file path')
@click.option('--force-reload', is_flag=True, help='Force reload data from BigQuery')
@click.option('--style', 
              type=click.Choice(['investigative', 'compliance', 'executive']),
              default='investigative',
              help='Report style')
@click.option('--format',
              type=click.Choice(['markdown', 'html']),
              default='markdown',
              help='Output format')
@click.option('--no-viz', is_flag=True, help='Skip visualization generation')
def analyze(config, force_reload, style, format, no_viz):
    """Run complete COI analysis pipeline"""
    try:
        click.echo(click.style('🚀 Starting Healthcare COI Analysis', fg='green', bold=True))
        click.echo(f"Configuration: {config}")
        click.echo(f"Report style: {style}")
        click.echo(f"Output format: {format}")
        
        # Run pipeline
        pipeline = FullAnalysisPipeline(config)
        results = pipeline.run(
            force_reload=force_reload,
            generate_visualizations=not no_viz,
            report_style=style,
            output_format=format
        )
        
        # Display results
        click.echo("\n" + "="*60)
        click.echo(click.style('✅ Analysis Complete!', fg='green', bold=True))
        click.echo("="*60)
        
        if 'open_payments' in results:
            op = results['open_payments'].get('overall_metrics', {})
            click.echo(f"\n📊 Open Payments:")
            click.echo(f"   Providers: {op.get('unique_providers', 0):,}")
            click.echo(f"   Total: ${op.get('total_payments', 0):,.0f}")
        
        if 'correlations' in results:
            corr = results['correlations'].get('influence_metrics', {})
            if corr:
                click.echo(f"\n🔍 Correlations:")
                click.echo(f"   Cost Influence: {corr.get('overall_rx_cost_influence', 1):.1f}x")
                click.echo(f"   ROI: {corr.get('overall_roi', 0):.1f}x")
        
        if 'risk_assessment' in results:
            risk = results['risk_assessment'].get('summary', {})
            click.echo(f"\n⚠️  Risk Assessment:")
            click.echo(f"   High-Risk Providers: {risk.get('high_risk_count', 0):,}")
        
        click.echo(f"\n📄 Report: {results.get('report_path', 'N/A')}")
        
    except Exception as e:
        click.echo(click.style(f'❌ Analysis failed: {e}', fg='red'), err=True)
        sys.exit(1)


@cli.command()
@click.option('--config', default='config/config.yaml', help='Configuration file path')
def validate(config):
    """Validate data quality and configuration"""
    try:
        click.echo(click.style('🔍 Validating Configuration and Data', fg='blue', bold=True))
        
        from src.data import DataLoader
        
        # Load and validate data
        loader = DataLoader(config)
        validator = DataValidator()
        
        # Check provider NPIs
        click.echo("\nValidating provider NPIs...")
        providers = loader.load_provider_npis()
        provider_report = validator.validate_provider_data(providers)
        
        if provider_report['passed']:
            click.echo(click.style('✅ Provider data valid', fg='green'))
        else:
            click.echo(click.style('⚠️  Provider data issues found', fg='yellow'))
        
        # Print summary
        validator.print_summary()
        
    except Exception as e:
        click.echo(click.style(f'❌ Validation failed: {e}', fg='red'), err=True)
        sys.exit(1)


@cli.command()
@click.option('--days', default=7, help='Remove cached files older than N days')
def clean(days):
    """Clean old cached and processed files"""
    try:
        click.echo(click.style(f'🧹 Cleaning files older than {days} days', fg='blue'))
        
        from src.data import DataLoader, BigQueryConnector
        
        # Clean data loader cache
        loader = DataLoader()
        loader.clean_old_files(days)
        
        # Clean BigQuery cache
        bq = BigQueryConnector()
        from datetime import timedelta
        bq.clear_cache(older_than=timedelta(days=days))
        
        click.echo(click.style('✅ Cleanup complete', fg='green'))
        
    except Exception as e:
        click.echo(click.style(f'❌ Cleanup failed: {e}', fg='red'), err=True)
        sys.exit(1)


@cli.command()
@click.argument('data_type', 
                type=click.Choice(['payments', 'prescriptions', 'correlations']))
@click.option('--output', help='Output file path')
def export(data_type, output):
    """Export analysis results to file"""
    try:
        click.echo(click.style(f'📤 Exporting {data_type} data', fg='blue'))
        
        from src.data import DataLoader
        import pandas as pd
        
        loader = DataLoader()
        
        # Load the requested data
        if data_type == 'payments':
            data = loader.load_open_payments()
        elif data_type == 'prescriptions':
            data = loader.load_prescriptions()
        elif data_type == 'correlations':
            data = loader.load_analysis_results('correlations')
        
        # Determine output path
        if not output:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output = f"exports/{data_type}_{timestamp}.csv"
        
        # Save data
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        
        if output.endswith('.parquet'):
            data.to_parquet(output, index=False)
        else:
            data.to_csv(output, index=False)
        
        click.echo(click.style(f'✅ Data exported to {output}', fg='green'))
        
    except Exception as e:
        click.echo(click.style(f'❌ Export failed: {e}', fg='red'), err=True)
        sys.exit(1)


@cli.command()
def info():
    """Display system and configuration information"""
    click.echo(click.style('Healthcare COI Analytics Template v2.0', fg='cyan', bold=True))
    click.echo('='*60)
    
    # Check Python version
    import sys
    click.echo(f"Python: {sys.version}")
    
    # Check key packages
    try:
        import pandas as pd
        click.echo(f"Pandas: {pd.__version__}")
    except:
        click.echo("Pandas: Not installed")
    
    try:
        import sklearn
        click.echo(f"Scikit-learn: {sklearn.__version__}")
    except:
        click.echo("Scikit-learn: Not installed")
    
    # Check configuration
    try:
        from src.data import DataLoader
        loader = DataLoader()
        config = loader.config
        click.echo(f"\nHealth System: {config['health_system']['name']}")
        click.echo(f"Analysis Period: {config['analysis']['start_year']}-{config['analysis']['end_year']}")
        click.echo(f"BigQuery Project: {config['bigquery']['project_id']}")
    except Exception as e:
        click.echo(f"\nConfiguration: Error loading ({e})")
    
    click.echo('='*60)


if __name__ == '__main__':
    cli()