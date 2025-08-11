#!/usr/bin/env python3
"""
BigQuery Data Pipeline for COI Compliance Metrics
DA-152: Build BigQuery Data Pipeline

This script handles the extraction, transformation, and loading of COI-related data
from BigQuery for compliance analysis and dashboard visualization.

Data Storage Strategy:
- Raw data: CSV format in /data/raw/ (for audit trail and debugging)
- Processed data: Parquet format in /data/processed/ (for performance)
- UI-ready data: Parquet format in /data/staging/ (optimized for dashboard)
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Ensure data directories exist
(DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "staging").mkdir(parents=True, exist_ok=True)
(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

class COIDataPipeline:
    """Main pipeline class for COI compliance data processing"""
    
    def __init__(self, project_id: str = None, credentials_path: str = None):
        """
        Initialize the pipeline with BigQuery connection
        
        Args:
            project_id: GCP project ID (defaults to env variable)
            credentials_path: Path to service account JSON (defaults to env variable)
        """
        self.project_id = project_id or os.getenv('GCP_PROJECT_ID', 'data-analytics-389803')
        
        # Load configurations
        self.load_configs()
        
        # Initialize BigQuery client
        if credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            credentials_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.bq_client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
        else:
            # Use default credentials (for cloud environments)
            self.bq_client = bigquery.Client(project=self.project_id)
            
        logger.info(f"Initialized BigQuery client for project: {self.project_id}")
        
    def load_configs(self):
        """Load YAML configuration files"""
        # Load policy configuration
        with open(CONFIG_DIR / "coi-policies.yaml", 'r') as f:
            self.policy_config = yaml.safe_load(f)
            
        # Load operational thresholds
        with open(CONFIG_DIR / "coi-operational-thresholds.yaml", 'r') as f:
            self.thresholds_config = yaml.safe_load(f)
            
        logger.info("Loaded configuration files")
        
    def extract_disclosure_data_with_op(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Extract disclosure data that already includes Open Payments information
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with integrated disclosure and Open Payments records
        """
        query = f"""
        SELECT
            -- Provider Information
            provider_id,
            provider_name,
            provider_npi,
            provider_specialty,
            provider_department,
            decision_authority_level,
            
            -- Disclosure Information
            disclosure_id,
            disclosure_date,
            disclosure_type,
            disclosure_status,
            
            -- Relationship Details
            entity_name,
            entity_type,
            relationship_type,
            relationship_start_date,
            relationship_ongoing,
            
            -- Financial Information
            financial_amount,
            financial_amount_type,
            equity_percentage,
            management_role,
            board_position,
            
            -- Open Payments Data (already integrated)
            open_payments_total,
            open_payments_count,
            open_payments_categories,
            open_payments_last_date,
            open_payments_matched,
            
            -- Risk Assessment
            calculated_risk_score,
            risk_tier_assigned,
            
            -- Review Status
            review_status,
            review_date,
            reviewer_id,
            management_plan_required,
            management_plan_status,
            recusal_required,
            
            -- Monitoring
            last_review_date,
            next_review_date,
            monitoring_frequency,
            
            -- Timestamps
            created_at,
            updated_at
        FROM
            `{self.project_id}.provider_disclosures.integrated_disclosures`
        WHERE
            disclosure_date BETWEEN '{start_date}' AND '{end_date}'
            OR (relationship_ongoing = TRUE AND disclosure_date <= '{end_date}')
        ORDER BY
            risk_tier_assigned DESC,
            financial_amount DESC,
            disclosure_date DESC
        """
        
        logger.info(f"Extracting integrated disclosure data from {start_date} to {end_date}")
        df = self.bq_client.query(query).to_dataframe()
        
        # Save raw data as CSV (for audit trail)
        csv_path = DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(df)} raw records to CSV: {csv_path}")
        
        # Also save raw data as Parquet for faster reprocessing
        parquet_path = DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.parquet"
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        logger.info(f"Saved {len(df)} raw records to Parquet: {parquet_path}")
        
        return df
        
    def extract_disclosure_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Extract internal disclosure forms data from BigQuery
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with disclosure records
        """
        query = f"""
        SELECT
            provider_id,
            provider_name,
            disclosure_date,
            disclosure_type,
            entity_name,
            relationship_type,
            financial_amount,
            equity_percentage,
            management_role,
            review_status,
            management_plan_required,
            last_review_date,
            next_review_date
        FROM
            `{self.project_id}.provider_disclosures.annual_disclosures`
        WHERE
            disclosure_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY
            disclosure_date DESC
        """
        
        logger.info(f"Extracting disclosure data from {start_date} to {end_date}")
        try:
            df = self.bq_client.query(query).to_dataframe()
            
            # Save raw data
            output_path = DATA_DIR / "raw" / "disclosures" / f"disclosures_{start_date}_{end_date}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(df)} disclosure records to {output_path}")
            
        except Exception as e:
            logger.warning(f"Could not extract disclosure data: {e}")
            logger.info("Creating empty disclosure DataFrame")
            df = pd.DataFrame(columns=[
                'provider_id', 'provider_name', 'disclosure_date', 'disclosure_type',
                'entity_name', 'relationship_type', 'financial_amount', 'equity_percentage',
                'management_role', 'review_status', 'management_plan_required',
                'last_review_date', 'next_review_date'
            ])
            
        return df
        
    def calculate_risk_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate risk scores based on operational thresholds
        
        Args:
            df: DataFrame with disclosure or payments data
            
        Returns:
            DataFrame with added risk scores and tiers
        """
        logger.info("Calculating risk scores")
        
        # Get scoring configuration
        scoring = self.thresholds_config['risk_scoring']
        weights = scoring['factor_weights']
        
        # Initialize risk columns
        df['financial_score'] = 0
        df['authority_score'] = 0
        df['relationship_score'] = 0
        df['overlap_score'] = 0
        df['frequency_score'] = 0
        
        # Calculate financial amount score
        if 'financial_amount' in df.columns:
            amount_col = 'financial_amount'
        elif 'payment_amount' in df.columns:
            amount_col = 'payment_amount'
        else:
            amount_col = None
            
        if amount_col:
            df['financial_score'] = df[amount_col].apply(
                lambda x: self._score_financial_amount(x)
            )
        
        # Calculate weighted total score
        df['risk_score'] = (
            df['financial_score'] * weights['financial_amount'] / 100 +
            df['authority_score'] * weights['decision_authority'] / 100 +
            df['relationship_score'] * weights['relationship_type'] / 100 +
            df['overlap_score'] * weights['service_overlap'] / 100 +
            df['frequency_score'] * weights['frequency'] / 100
        )
        
        # Assign risk tiers
        df['risk_tier'] = df['risk_score'].apply(self._assign_risk_tier)
        
        # Determine management requirements
        df['review_required'] = df['risk_tier'].apply(
            lambda x: x in ['moderate', 'high', 'critical']
        )
        df['management_plan_required'] = df['risk_tier'].apply(
            lambda x: x in ['high', 'critical']
        )
        
        return df
        
    def _score_financial_amount(self, amount: float) -> int:
        """Score financial amount based on thresholds"""
        if pd.isna(amount) or amount <= 0:
            return 0
            
        scale = self.thresholds_config['risk_scoring']['financial_amount_scale']
        
        if amount <= 1000:
            return scale['minimal']['score']
        elif amount <= 5000:
            return scale['low']['score']
        elif amount <= 25000:
            return scale['moderate']['score']
        elif amount <= 100000:
            return scale['high']['score']
        else:
            return scale['critical']['score']
            
    def _assign_risk_tier(self, score: float) -> str:
        """Assign risk tier based on score"""
        if score <= 25:
            return 'low'
        elif score <= 50:
            return 'moderate'
        elif score <= 75:
            return 'high'
        else:
            return 'critical'
            
    def prepare_ui_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare optimized data for UI consumption
        
        Args:
            df: Processed disclosure DataFrame
            
        Returns:
            DataFrame optimized for UI display
        """
        logger.info("Preparing UI-optimized data")
        
        # Select only columns needed for UI
        ui_columns = [
            'provider_id', 'provider_name', 'provider_npi', 'specialty', 'department',
            'entity_name', 'relationship_type', 'financial_amount', 'open_payments_total',
            'risk_tier', 'risk_score', 'review_status', 'management_plan_required',
            'last_review_date', 'next_review_date', 'disclosure_date',
            'decision_authority_level', 'open_payments_matched'
        ]
        
        # Filter to existing columns
        ui_columns = [col for col in ui_columns if col in df.columns]
        ui_df = df[ui_columns].copy()
        
        # Optimize data types for storage
        # Convert strings to categories for repeated values
        categorical_columns = ['specialty', 'department', 'entity_name', 'relationship_type',
                              'risk_tier', 'review_status', 'decision_authority_level']
        
        for col in categorical_columns:
            if col in ui_df.columns:
                ui_df[col] = ui_df[col].astype('category')
        
        # Ensure proper date formats
        date_columns = ['last_review_date', 'next_review_date', 'disclosure_date']
        for col in date_columns:
            if col in ui_df.columns:
                ui_df[col] = pd.to_datetime(ui_df[col], errors='coerce')
        
        # Sort by risk priority for UI display
        sort_order = {'critical': 0, 'high': 1, 'moderate': 2, 'low': 3}
        if 'risk_tier' in ui_df.columns:
            ui_df['risk_priority'] = ui_df['risk_tier'].map(sort_order).fillna(4)
            ui_df = ui_df.sort_values(['risk_priority', 'financial_amount'], ascending=[True, False])
            ui_df = ui_df.drop('risk_priority', axis=1)
        
        return ui_df
    
    def save_processed_data(self, df: pd.DataFrame, data_type: str, start_date: str, end_date: str):
        """
        Save processed data in both Parquet (for performance) and JSON (for web compatibility)
        
        Args:
            df: Processed DataFrame
            data_type: Type of data ('processed' or 'ui_ready')
            start_date: Start date for filename
            end_date: End date for filename
        """
        if df.empty:
            logger.warning(f"No data to save for {data_type}")
            return
            
        # Determine save directory
        if data_type == 'processed':
            save_dir = DATA_DIR / "processed"
        else:
            save_dir = DATA_DIR / "staging"
        
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as Parquet for performance
        parquet_path = save_dir / f"disclosures_{data_type}_{start_date}_{end_date}.parquet"
        df.to_parquet(
            parquet_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        logger.info(f"Saved {len(df)} records to Parquet: {parquet_path}")
        
        # For UI data, also save as JSON for web consumption
        if data_type == 'ui_ready':
            # Convert dates to strings for JSON serialization
            df_json = df.copy()
            for col in df_json.select_dtypes(include=['datetime64']).columns:
                df_json[col] = df_json[col].dt.strftime('%Y-%m-%d')
            
            # Convert categories back to strings for JSON
            for col in df_json.select_dtypes(include=['category']).columns:
                df_json[col] = df_json[col].astype(str)
            
            # Create JSON structure for UI
            json_data = {
                'metadata': {
                    'generated': datetime.now().isoformat(),
                    'record_count': len(df_json),
                    'data_range': f"{start_date} to {end_date}",
                    'risk_distribution': df_json['risk_tier'].value_counts().to_dict() if 'risk_tier' in df_json else {}
                },
                'disclosures': df_json.to_dict('records')
            }
            
            json_path = save_dir / f"disclosure_data.json"
            with open(json_path, 'w') as f:
                json.dump(json_data, f, indent=2, default=str)
            logger.info(f"Saved UI data to JSON: {json_path}")
    
    def aggregate_metrics(self, payments_df: pd.DataFrame, disclosures_df: pd.DataFrame) -> Dict:
        """
        Aggregate data for dashboard metrics
        
        Args:
            payments_df: Open Payments data
            disclosures_df: Disclosure forms data
            
        Returns:
            Dictionary of aggregated metrics
        """
        logger.info("Aggregating metrics for dashboard")
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'open_payments': {},
            'disclosures': {},
            'combined': {},
            'risk_distribution': {}
        }
        
        # Open Payments metrics
        if not payments_df.empty:
            metrics['open_payments'] = {
                'total_payments': len(payments_df),
                'unique_providers': payments_df['physician_profile_id'].nunique(),
                'total_amount': float(payments_df['payment_amount'].sum()),
                'average_payment': float(payments_df['payment_amount'].mean()),
                'median_payment': float(payments_df['payment_amount'].median()),
                'payments_by_nature': payments_df['payment_nature'].value_counts().to_dict(),
                'top_payers': payments_df.groupby('payer_name')['payment_amount'].sum().nlargest(10).to_dict()
            }
        
        # Disclosure metrics
        if not disclosures_df.empty:
            metrics['disclosures'] = {
                'total_disclosures': len(disclosures_df),
                'unique_providers': disclosures_df['provider_id'].nunique(),
                'relationships_by_type': disclosures_df['relationship_type'].value_counts().to_dict(),
                'requiring_review': disclosures_df['review_required'].sum() if 'review_required' in disclosures_df else 0,
                'management_plans': disclosures_df['management_plan_required'].sum() if 'management_plan_required' in disclosures_df else 0
            }
        
        # Risk distribution
        if 'risk_tier' in payments_df.columns:
            risk_dist = payments_df['risk_tier'].value_counts()
            metrics['risk_distribution'] = {
                'low': int(risk_dist.get('low', 0)),
                'moderate': int(risk_dist.get('moderate', 0)),
                'high': int(risk_dist.get('high', 0)),
                'critical': int(risk_dist.get('critical', 0))
            }
            
        # Compliance rates
        total_providers = max(
            metrics['open_payments'].get('unique_providers', 0),
            metrics['disclosures'].get('unique_providers', 0)
        )
        
        if total_providers > 0:
            metrics['combined']['compliance_rate'] = (
                metrics['disclosures'].get('unique_providers', 0) / total_providers * 100
            )
        
        return metrics
        
    def generate_compliance_report(self, metrics: Dict) -> str:
        """
        Generate a compliance report summary
        
        Args:
            metrics: Aggregated metrics dictionary
            
        Returns:
            Path to generated report
        """
        logger.info("Generating compliance report")
        
        report_date = datetime.now().strftime("%Y-%m-%d")
        report_path = OUTPUT_DIR / f"coi_compliance_report_{report_date}.json"
        
        # Add threshold information to report
        metrics['thresholds'] = {
            'disclosure_trigger': self.thresholds_config['disclosure_triggers']['payments']['annual_aggregate']['threshold'],
            'risk_tiers': {
                tier: self.thresholds_config['risk_tiers'][tier]['range_min']
                for tier in ['tier_1_low', 'tier_2_moderate', 'tier_3_high', 'tier_4_critical']
            }
        }
        
        # Save report
        with open(report_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
            
        logger.info(f"Report saved to {report_path}")
        return str(report_path)
        
    def run_pipeline(self, start_date: str, end_date: str, skip_bigquery: bool = False, use_parquet: bool = True):
        """
        Run the complete data pipeline with optimized storage
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            skip_bigquery: Skip BigQuery extraction (use cached data)
            use_parquet: Use Parquet format for cached data loading
        """
        logger.info(f"Starting COI data pipeline for {start_date} to {end_date}")
        logger.info(f"Storage strategy: Raw CSV + Processed Parquet + UI JSON")
        
        # Extract or load cached data
        if not skip_bigquery:
            # Extract from BigQuery (saves both CSV and Parquet)
            disclosures_df = self.extract_disclosure_data_with_op(start_date, end_date)
        else:
            logger.info("Using cached data (skip_bigquery=True)")
            
            # Try to load from Parquet first (faster), fallback to CSV
            if use_parquet:
                parquet_path = DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.parquet"
                if parquet_path.exists():
                    logger.info(f"Loading cached data from Parquet: {parquet_path}")
                    disclosures_df = pd.read_parquet(parquet_path, engine='pyarrow')
                else:
                    use_parquet = False
            
            if not use_parquet:
                csv_path = DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.csv"
                if csv_path.exists():
                    logger.info(f"Loading cached data from CSV: {csv_path}")
                    disclosures_df = pd.read_csv(csv_path)
                else:
                    logger.warning("No cached data found, creating empty DataFrame")
                    disclosures_df = pd.DataFrame()
        
        if disclosures_df.empty:
            logger.warning("No disclosure data to process")
            return {}
        
        # Calculate risk scores and enrich data
        logger.info("Calculating risk scores and enriching data")
        processed_df = self.calculate_risk_scores(disclosures_df)
        
        # Save processed data as Parquet
        self.save_processed_data(processed_df, 'processed', start_date, end_date)
        
        # Prepare UI-optimized data
        ui_df = self.prepare_ui_data(processed_df)
        
        # Save UI-ready data (both Parquet and JSON)
        self.save_processed_data(ui_df, 'ui_ready', start_date, end_date)
        
        # Aggregate metrics for dashboard
        metrics = self.aggregate_metrics(pd.DataFrame(), processed_df)
        
        # Generate compliance report
        report_path = self.generate_compliance_report(metrics)
        
        # Performance statistics
        logger.info("=== Storage Summary ===")
        if not skip_bigquery:
            csv_size = (DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.csv").stat().st_size / 1024 / 1024
            parquet_size = (DATA_DIR / "raw" / "disclosures" / f"integrated_disclosures_{start_date}_{end_date}.parquet").stat().st_size / 1024 / 1024
            logger.info(f"Raw CSV size: {csv_size:.2f} MB")
            logger.info(f"Raw Parquet size: {parquet_size:.2f} MB")
            logger.info(f"Compression ratio: {csv_size/parquet_size:.2f}x")
        
        logger.info("Pipeline completed successfully")
        return metrics


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='COI BigQuery Data Pipeline')
    parser.add_argument(
        '--start-date',
        type=str,
        default=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--project-id',
        type=str,
        help='GCP Project ID'
    )
    parser.add_argument(
        '--credentials',
        type=str,
        help='Path to service account credentials JSON'
    )
    parser.add_argument(
        '--skip-bigquery',
        action='store_true',
        help='Skip BigQuery extraction and use cached data'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize and run pipeline
        pipeline = COIDataPipeline(
            project_id=args.project_id,
            credentials_path=args.credentials
        )
        
        metrics = pipeline.run_pipeline(
            start_date=args.start_date,
            end_date=args.end_date,
            skip_bigquery=args.skip_bigquery
        )
        
        # Print summary
        print("\n=== COI Compliance Pipeline Summary ===")
        print(f"Date Range: {args.start_date} to {args.end_date}")
        
        if 'open_payments' in metrics and metrics['open_payments']:
            print(f"\nOpen Payments:")
            print(f"  - Total Payments: {metrics['open_payments']['total_payments']:,}")
            print(f"  - Unique Providers: {metrics['open_payments']['unique_providers']:,}")
            print(f"  - Total Amount: ${metrics['open_payments']['total_amount']:,.2f}")
            
        if 'risk_distribution' in metrics and metrics['risk_distribution']:
            print(f"\nRisk Distribution:")
            for tier, count in metrics['risk_distribution'].items():
                print(f"  - {tier.capitalize()}: {count}")
                
        if 'combined' in metrics and 'compliance_rate' in metrics['combined']:
            print(f"\nCompliance Rate: {metrics['combined']['compliance_rate']:.1f}%")
        
        print("\n✓ Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()