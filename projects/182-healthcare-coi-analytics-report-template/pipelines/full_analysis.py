"""
Full Analysis Pipeline
Complete end-to-end healthcare COI analysis
"""

import sys
from pathlib import Path
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.data import BigQueryConnector, DataLoader, DataValidator
from src.analysis import (
    OpenPaymentsAnalyzer,
    PrescriptionAnalyzer,
    CorrelationAnalyzer,
    RiskScorer,
    SpecialtyAnalyzer
)
from src.reporting import ReportGenerator, VisualizationGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FullAnalysisPipeline:
    """Orchestrates complete healthcare COI analysis pipeline"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize pipeline with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.data_loader = DataLoader(config_path)
        self.validator = DataValidator()
        self.results = {}
        
    def run(
        self,
        force_reload: bool = False,
        generate_visualizations: bool = True,
        report_style: str = "investigative",
        output_format: str = "markdown"
    ) -> Dict[str, Any]:
        """
        Run complete analysis pipeline
        
        Args:
            force_reload: Force reload data from BigQuery
            generate_visualizations: Whether to generate charts
            report_style: Style of report to generate
            output_format: Output format for report
            
        Returns:
            Dictionary with analysis results and report path
        """
        logger.info("="*60)
        logger.info("STARTING FULL HEALTHCARE COI ANALYSIS PIPELINE")
        logger.info("="*60)
        
        try:
            # Step 1: Load and validate data
            logger.info("\n[Step 1/7] Loading and validating data...")
            data = self._load_and_validate_data(force_reload)
            
            # Step 2: Analyze Open Payments
            logger.info("\n[Step 2/7] Analyzing Open Payments...")
            self.results['open_payments'] = self._analyze_open_payments(data['payments'])
            
            # Step 3: Analyze Prescriptions
            logger.info("\n[Step 3/7] Analyzing prescription patterns...")
            self.results['prescriptions'] = self._analyze_prescriptions(data['prescriptions'])
            
            # Step 4: Analyze Correlations
            logger.info("\n[Step 4/7] Analyzing payment-prescription correlations...")
            self.results['correlations'] = self._analyze_correlations(
                data['payments'], data['prescriptions']
            )
            
            # Step 5: Risk Assessment
            logger.info("\n[Step 5/7] Performing risk assessment...")
            self.results['risk_assessment'] = self._assess_risks(
                data['payments'], data['prescriptions']
            )
            
            # Step 6: Specialty Analysis
            logger.info("\n[Step 6/7] Analyzing specialty patterns...")
            self.results['specialty_analysis'] = self._analyze_specialties(
                data['payments'], data['prescriptions']
            )
            
            # Step 7: Generate visualizations
            if generate_visualizations:
                logger.info("\n[Step 7/7] Generating visualizations...")
                self.results['visualizations'] = self._generate_visualizations()
            
            # Generate report
            logger.info("\n[Final Step] Generating report...")
            report_path = self._generate_report(report_style, output_format)
            self.results['report_path'] = report_path
            
            # Print summary
            self._print_summary()
            
            logger.info("\n" + "="*60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            
            return self.results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def _load_and_validate_data(self, force_reload: bool) -> Dict[str, Any]:
        """Load and validate all required data"""
        data = {}
        
        # Load provider NPIs
        logger.info("Loading provider NPIs...")
        data['providers'] = self.data_loader.load_provider_npis(force_reload)
        self.validator.validate_provider_data(data['providers'])
        
        # Load Open Payments
        logger.info("Loading Open Payments data...")
        data['payments'] = self.data_loader.load_open_payments(force_reload=force_reload)
        self.validator.validate_payment_data(data['payments'])
        
        # Load Prescriptions
        logger.info("Loading prescription data...")
        data['prescriptions'] = self.data_loader.load_prescriptions(force_reload=force_reload)
        self.validator.validate_prescription_data(data['prescriptions'])
        
        # Print validation summary
        self.validator.print_summary()
        
        return data
    
    def _analyze_open_payments(self, payments_data):
        """Analyze Open Payments data"""
        analyzer = OpenPaymentsAnalyzer(payments_data)
        results = analyzer.analyze_all()
        
        # Log key findings
        metrics = results.get('overall_metrics', {})
        logger.info(f"Analyzed {metrics.get('unique_providers', 0):,} providers")
        logger.info(f"Total payments: ${metrics.get('total_payments', 0):,.0f}")
        
        return results
    
    def _analyze_prescriptions(self, prescription_data):
        """Analyze prescription patterns"""
        analyzer = PrescriptionAnalyzer(prescription_data)
        results = analyzer.analyze_all()
        
        # Log key findings
        metrics = results.get('overall_metrics', {})
        logger.info(f"Analyzed {metrics.get('unique_prescribers', 0):,} prescribers")
        logger.info(f"Total prescription value: ${metrics.get('total_prescription_value', 0):,.0f}")
        
        return results
    
    def _analyze_correlations(self, payments_data, prescription_data):
        """Analyze payment-prescription correlations"""
        analyzer = CorrelationAnalyzer(payments_data, prescription_data)
        results = analyzer.analyze_all()
        
        # Log key findings
        influence = results.get('influence_metrics', {})
        if influence:
            logger.info(f"Overall cost influence: {influence.get('overall_rx_cost_influence', 1):.1f}x")
            logger.info(f"Overall ROI: {influence.get('overall_roi', 0):.1f}x")
        
        return results
    
    def _assess_risks(self, payments_data, prescription_data):
        """Perform risk assessment"""
        config = self.data_loader.config
        scorer = RiskScorer(config)
        
        # Calculate risk scores
        risk_scores = scorer.score_providers(payments_data, prescription_data)
        
        # Generate risk report
        risk_report = scorer.generate_risk_report()
        
        # Log key findings
        summary = risk_report.get('summary', {})
        logger.info(f"High-risk providers: {summary.get('high_risk_count', 0):,}")
        logger.info(f"Anomalies detected: {summary.get('anomaly_count', 0):,}")
        
        return risk_report
    
    def _analyze_specialties(self, payments_data, prescription_data):
        """Analyze specialty-specific patterns"""
        analyzer = SpecialtyAnalyzer(payments_data, prescription_data)
        results = analyzer.analyze_all()
        
        # Log key findings
        overview = results.get('specialty_overview', None)
        if overview is not None and not overview.empty:
            logger.info(f"Analyzed {len(overview)} specialties")
        
        return results
    
    def _generate_visualizations(self):
        """Generate visualization charts"""
        viz_gen = VisualizationGenerator()
        figures = viz_gen.generate_all_visualizations(self.results)
        
        logger.info(f"Generated {len(figures)} visualizations")
        return figures
    
    def _generate_report(self, report_style: str, output_format: str) -> str:
        """Generate final report"""
        report_gen = ReportGenerator(self.config_path)
        report_path = report_gen.generate_report(
            self.results,
            report_style=report_style,
            output_format=output_format
        )
        
        logger.info(f"Report generated: {report_path}")
        return report_path
    
    def _print_summary(self):
        """Print analysis summary"""
        print("\n" + "="*60)
        print("ANALYSIS SUMMARY")
        print("="*60)
        
        if 'open_payments' in self.results:
            op = self.results['open_payments'].get('overall_metrics', {})
            print(f"\nOpen Payments:")
            print(f"  Providers: {op.get('unique_providers', 0):,}")
            print(f"  Total: ${op.get('total_payments', 0):,.0f}")
        
        if 'prescriptions' in self.results:
            rx = self.results['prescriptions'].get('overall_metrics', {})
            print(f"\nPrescriptions:")
            print(f"  Prescribers: {rx.get('unique_prescribers', 0):,}")
            print(f"  Total Value: ${rx.get('total_prescription_value', 0):,.0f}")
        
        if 'correlations' in self.results:
            corr = self.results['correlations'].get('influence_metrics', {})
            if corr:
                print(f"\nCorrelations:")
                print(f"  Cost Influence: {corr.get('overall_rx_cost_influence', 1):.1f}x")
                print(f"  ROI: {corr.get('overall_roi', 0):.1f}x")
        
        if 'risk_assessment' in self.results:
            risk = self.results['risk_assessment'].get('summary', {})
            print(f"\nRisk Assessment:")
            print(f"  High-Risk Providers: {risk.get('high_risk_count', 0):,}")
            print(f"  Anomalies: {risk.get('anomaly_count', 0):,}")
        
        if 'report_path' in self.results:
            print(f"\nReport: {self.results['report_path']}")
        
        print("="*60)


def main():
    """Main entry point for full analysis pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run full healthcare COI analysis')
    parser.add_argument('--config', default='config/config.yaml', help='Config file path')
    parser.add_argument('--force-reload', action='store_true', help='Force reload from BigQuery')
    parser.add_argument('--no-viz', action='store_true', help='Skip visualizations')
    parser.add_argument('--style', default='investigative', 
                       choices=['investigative', 'compliance', 'executive'],
                       help='Report style')
    parser.add_argument('--format', default='markdown',
                       choices=['markdown', 'html'],
                       help='Output format')
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = FullAnalysisPipeline(args.config)
    results = pipeline.run(
        force_reload=args.force_reload,
        generate_visualizations=not args.no_viz,
        report_style=args.style,
        output_format=args.format
    )
    
    return 0 if results else 1


if __name__ == '__main__':
    sys.exit(main())