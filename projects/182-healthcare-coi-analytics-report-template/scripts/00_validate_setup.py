#!/usr/bin/env python3
"""
Validate environment setup and data before running analysis
"""

import os
import sys
import json
import yaml
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import logging
from datetime import datetime

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent
sys.path.append(str(TEMPLATE_DIR))

# Load environment variables
load_dotenv(TEMPLATE_DIR / '.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from CONFIG.yaml"""
    config_path = TEMPLATE_DIR / 'CONFIG.yaml'
    if not config_path.exists():
        logger.error(f"CONFIG.yaml not found at {config_path}")
        return None
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def validate_environment():
    """Validate environment variables and GCP credentials"""
    logger.info("=" * 60)
    logger.info("VALIDATING ENVIRONMENT")
    logger.info("=" * 60)
    
    issues = []
    
    # Check for GCP credentials
    gcp_key = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not gcp_key:
        issues.append("❌ GCP_SERVICE_ACCOUNT_KEY not found in environment")
    else:
        try:
            key_data = json.loads(gcp_key)
            if 'project_id' in key_data:
                logger.info(f"✅ GCP credentials found for project: {key_data['project_id']}")
            else:
                issues.append("❌ Invalid GCP service account key format")
        except json.JSONDecodeError:
            issues.append("❌ GCP_SERVICE_ACCOUNT_KEY is not valid JSON")
    
    # Check Python packages
    required_packages = ['pandas', 'numpy', 'google.cloud.bigquery', 'yaml', 'openpyxl', 'matplotlib']
    for package in required_packages:
        try:
            __import__(package.replace('.', '_'))
            logger.info(f"✅ Package '{package}' is installed")
        except ImportError:
            issues.append(f"❌ Required package '{package}' is not installed")
    
    return issues

def validate_config(config):
    """Validate configuration file"""
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATING CONFIGURATION")
    logger.info("=" * 60)
    
    issues = []
    
    # Check required sections
    required_sections = ['health_system', 'analysis', 'bigquery', 'thresholds']
    for section in required_sections:
        if section not in config:
            issues.append(f"❌ Missing required config section: {section}")
        else:
            logger.info(f"✅ Config section '{section}' found")
    
    # Check health system name
    if '{{' in str(config.get('health_system', {}).get('name', '')):
        issues.append("❌ Health system name not configured (still has placeholder)")
    else:
        logger.info(f"✅ Health system: {config['health_system']['name']}")
    
    # Check analysis years
    start_year = config.get('analysis', {}).get('start_year')
    end_year = config.get('analysis', {}).get('end_year')
    if start_year and end_year:
        logger.info(f"✅ Analysis period: {start_year} - {end_year}")
        if end_year - start_year > 10:
            issues.append(f"⚠️  Warning: Large analysis period ({end_year - start_year + 1} years)")
    
    return issues

def validate_npi_file(config):
    """Validate NPI input file"""
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATING NPI FILE")
    logger.info("=" * 60)
    
    issues = []
    
    npi_file = config.get('health_system', {}).get('npi_file')
    if not npi_file:
        issues.append("❌ NPI file not specified in config")
        return issues
    
    npi_path = TEMPLATE_DIR / npi_file
    if not npi_path.exists():
        issues.append(f"❌ NPI file not found: {npi_path}")
        logger.info(f"   Expected location: {npi_path}")
        logger.info(f"   Please place your provider NPI list at this location")
        return issues
    
    # Try to read the NPI file
    try:
        df = pd.read_csv(npi_path)
        logger.info(f"✅ NPI file found with {len(df)} rows")
        
        # Check for required columns
        required_cols = ['NPI']
        recommended_cols = ['Full_Name', 'Primary_Specialty']
        
        for col in required_cols:
            if col not in df.columns:
                issues.append(f"❌ Required column '{col}' not found in NPI file")
            else:
                logger.info(f"✅ Required column '{col}' found")
        
        for col in recommended_cols:
            if col not in df.columns:
                logger.info(f"⚠️  Recommended column '{col}' not found (will be fetched from BigQuery if available)")
            else:
                logger.info(f"✅ Recommended column '{col}' found")
        
        # Check NPI validity (basic check)
        if 'NPI' in df.columns:
            invalid_npis = df[~df['NPI'].astype(str).str.match(r'^\d{10}$', na=False)]
            if len(invalid_npis) > 0:
                issues.append(f"⚠️  Found {len(invalid_npis)} invalid NPI values")
                logger.info(f"   Invalid NPIs: {invalid_npis['NPI'].head().tolist()}")
        
        # Check for duplicates
        if 'NPI' in df.columns:
            duplicates = df['NPI'].duplicated().sum()
            if duplicates > 0:
                issues.append(f"⚠️  Found {duplicates} duplicate NPIs")
        
        # Provider count check
        min_providers = config.get('quality_checks', {}).get('minimum_providers', 100)
        if len(df) < min_providers:
            issues.append(f"⚠️  Warning: Only {len(df)} providers (recommended minimum: {min_providers})")
        
    except Exception as e:
        issues.append(f"❌ Error reading NPI file: {str(e)}")
    
    return issues

def validate_directories():
    """Ensure all required directories exist"""
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATING DIRECTORY STRUCTURE")
    logger.info("=" * 60)
    
    required_dirs = [
        'data/inputs',
        'data/processed',
        'data/output',
        'logs',
        'data/.cache'
    ]
    
    for dir_path in required_dirs:
        full_path = TEMPLATE_DIR / dir_path
        if not full_path.exists():
            full_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Created directory: {dir_path}")
        else:
            logger.info(f"✅ Directory exists: {dir_path}")
    
    return []

def test_bigquery_connection(config):
    """Test BigQuery connection"""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING BIGQUERY CONNECTION")
    logger.info("=" * 60)
    
    issues = []
    
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            issues.append("❌ Cannot test BigQuery - no credentials")
            return issues
        
        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        
        project_id = config.get('bigquery', {}).get('project_id')
        client = bigquery.Client(project=project_id, credentials=credentials)
        
        # Test with a simple query
        query = "SELECT 1 as test"
        result = client.query(query).result()
        logger.info("✅ BigQuery connection successful")
        
        # Check if Open Payments table exists
        op_table = config.get('bigquery', {}).get('tables', {}).get('open_payments')
        if op_table:
            dataset_id = config.get('bigquery', {}).get('dataset')
            table_ref = f"{project_id}.{dataset_id}.{op_table}"
            try:
                query = f"SELECT COUNT(*) as count FROM `{table_ref}` LIMIT 1"
                result = client.query(query).result()
                for row in result:
                    logger.info(f"✅ Open Payments table accessible ({row.count:,} rows)")
            except Exception as e:
                issues.append(f"⚠️  Cannot access Open Payments table: {str(e)}")
        
    except Exception as e:
        issues.append(f"❌ BigQuery connection failed: {str(e)}")
    
    return issues

def generate_validation_report(all_issues):
    """Generate validation report"""
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    if not all_issues:
        logger.info("✅ ALL VALIDATIONS PASSED!")
        logger.info("   Your environment is ready to run the analysis.")
        return True
    else:
        logger.error(f"❌ VALIDATION FAILED - {len(all_issues)} issues found:")
        for i, issue in enumerate(all_issues, 1):
            logger.error(f"   {i}. {issue}")
        
        logger.info("\n📝 NEXT STEPS:")
        if any("GCP_SERVICE_ACCOUNT_KEY" in issue for issue in all_issues):
            logger.info("   1. Copy .env.example to .env")
            logger.info("   2. Add your GCP service account key to .env")
        
        if any("NPI file" in issue for issue in all_issues):
            logger.info("   1. Create a CSV file with your provider NPIs")
            logger.info("   2. Place it at the location specified in CONFIG.yaml")
            logger.info("   3. Ensure it has an 'NPI' column")
        
        if any("Health system name" in issue for issue in all_issues):
            logger.info("   1. Edit CONFIG.yaml")
            logger.info("   2. Replace {{HEALTH_SYSTEM_NAME}} with your actual health system name")
        
        if any("package" in issue.lower() for issue in all_issues):
            logger.info("   1. Run: pip install -r requirements.txt")
        
        return False

def main():
    """Run all validations"""
    logger.info("Healthcare COI Analytics - Setup Validation")
    logger.info(f"Template Directory: {TEMPLATE_DIR}")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_issues = []
    
    # Load configuration
    config = load_config()
    if not config:
        all_issues.append("❌ Cannot load CONFIG.yaml")
        generate_validation_report(all_issues)
        sys.exit(1)
    
    # Run validations
    all_issues.extend(validate_environment())
    all_issues.extend(validate_config(config))
    all_issues.extend(validate_directories())
    all_issues.extend(validate_npi_file(config))
    
    # Optional: test BigQuery connection
    if not all_issues:  # Only test if other validations pass
        all_issues.extend(test_bigquery_connection(config))
    
    # Generate report
    success = generate_validation_report(all_issues)
    
    # Save validation results
    results_file = TEMPLATE_DIR / 'data' / 'processed' / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(results_file, 'w') as f:
        f.write(f"Validation Report - {datetime.now()}\n")
        f.write("=" * 60 + "\n")
        if success:
            f.write("STATUS: PASSED\n")
        else:
            f.write("STATUS: FAILED\n")
            f.write(f"\nIssues ({len(all_issues)}):\n")
            for issue in all_issues:
                f.write(f"  - {issue}\n")
    
    logger.info(f"\n📄 Validation report saved to: {results_file}")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()