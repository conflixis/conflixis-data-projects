#!/usr/bin/env python3
"""
Run Full Healthcare COI Analysis Pipeline
This script orchestrates the complete analysis from start to finish
"""

import subprocess
import sys
import os
from pathlib import Path
import time
from datetime import datetime
import logging
import yaml

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent
sys.path.append(str(TEMPLATE_DIR))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the analysis pipeline scripts in order
PIPELINE_SCRIPTS = [
    {
        'name': 'Environment Validation',
        'script': '00_validate_setup.py',
        'required': True,
        'description': 'Validates environment setup and data'
    },
    {
        'name': 'Open Payments Analysis',
        'script': '01_analyze_op_payments.py',
        'required': True,
        'description': 'Analyzes industry payments to providers'
    },
    {
        'name': 'Prescription Pattern Analysis',
        'script': '02_analyze_prescriptions.py',
        'required': True,
        'description': 'Analyzes prescription patterns and trends'
    },
    {
        'name': 'Payment-Prescription Correlation',
        'script': '03_payment_influence.py',
        'required': True,
        'description': 'Analyzes correlations between payments and prescribing'
    },
    {
        'name': 'Report Generation',
        'script': '05_generate_report.py',
        'required': True,
        'description': 'Generates final analysis report'
    }
]

def print_header():
    """Print analysis header"""
    print("\n" + "=" * 70)
    print(" HEALTHCARE COI ANALYTICS - FULL ANALYSIS PIPELINE")
    print("=" * 70)
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

def print_step_header(step_num, total_steps, name, description):
    """Print step header"""
    print("\n" + "-" * 70)
    print(f" STEP {step_num}/{total_steps}: {name}")
    print(f" {description}")
    print("-" * 70)

def run_script(script_path, step_name):
    """
    Run a Python script and capture output
    
    Args:
        script_path: Path to the script
        step_name: Name of the step for logging
        
    Returns:
        Tuple of (success, duration)
    """
    start_time = time.time()
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(script_path.parent)
        )
        
        duration = time.time() - start_time
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        # Check for errors
        if result.returncode != 0:
            print(f"\n❌ ERROR in {step_name}:")
            if result.stderr:
                print(result.stderr)
            return False, duration
        
        print(f"\n✅ {step_name} completed successfully ({duration:.1f}s)")
        return True, duration
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n❌ ERROR running {step_name}: {str(e)}")
        return False, duration

def load_config():
    """Load configuration to display analysis parameters"""
    config_path = TEMPLATE_DIR / 'CONFIG.yaml'
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.warning(f"Could not load config: {e}")
        return None

def print_configuration(config):
    """Print analysis configuration"""
    if config:
        print("\n📋 CONFIGURATION:")
        print(f"   Health System: {config.get('health_system', {}).get('name', 'Not configured')}")
        print(f"   Analysis Period: {config.get('analysis', {}).get('start_year', 'N/A')} - {config.get('analysis', {}).get('end_year', 'N/A')}")
        print(f"   NPI File: {config.get('health_system', {}).get('npi_file', 'Not specified')}")
        
        # Check which components are enabled
        components = config.get('analysis', {}).get('components', {})
        enabled = [k for k, v in components.items() if v]
        if enabled:
            print(f"   Enabled Components: {', '.join(enabled)}")
    print()

def print_summary(results, total_duration):
    """Print analysis summary"""
    print("\n" + "=" * 70)
    print(" ANALYSIS COMPLETE")
    print("=" * 70)
    
    # Count successes and failures
    successful = sum(1 for r in results if r['success'])
    failed = sum(1 for r in results if not r['success'])
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total Steps: {len(results)}")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    print(f"   Total Duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
    
    # List any failed steps
    if failed > 0:
        print(f"\n⚠️  FAILED STEPS:")
        for r in results:
            if not r['success']:
                print(f"   - {r['name']}")
    
    # Output location
    output_dir = TEMPLATE_DIR / 'data' / 'output'
    if output_dir.exists():
        reports = list(output_dir.glob('*.md'))
        if reports:
            latest_report = max(reports, key=lambda x: x.stat().st_mtime)
            print(f"\n📄 LATEST REPORT: {latest_report}")
    
    print("\n" + "=" * 70)
    
    return successful == len(results)

def check_prerequisites():
    """Check if required files and directories exist"""
    print("\n🔍 Checking prerequisites...")
    
    issues = []
    
    # Check for .env file
    env_file = TEMPLATE_DIR / '.env'
    if not env_file.exists():
        issues.append("   ❌ .env file not found (copy from .env.example)")
    else:
        print("   ✅ .env file exists")
    
    # Check for CONFIG.yaml
    config_file = TEMPLATE_DIR / 'CONFIG.yaml'
    if not config_file.exists():
        issues.append("   ❌ CONFIG.yaml not found")
    else:
        print("   ✅ CONFIG.yaml exists")
    
    # Check for NPI file
    config = load_config()
    if config:
        npi_file = TEMPLATE_DIR / config.get('health_system', {}).get('npi_file', '')
        if npi_file and npi_file.exists():
            print(f"   ✅ NPI file exists: {npi_file.name}")
        else:
            issues.append(f"   ❌ NPI file not found: {npi_file}")
    
    # Check for required directories
    required_dirs = ['data/inputs', 'data/processed', 'data/output']
    for dir_path in required_dirs:
        full_path = TEMPLATE_DIR / dir_path
        if not full_path.exists():
            full_path.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ Created directory: {dir_path}")
        else:
            print(f"   ✅ Directory exists: {dir_path}")
    
    if issues:
        print("\n⚠️  ISSUES FOUND:")
        for issue in issues:
            print(issue)
        return False
    
    return True

def main():
    """Run the full analysis pipeline"""
    
    # Print header
    print_header()
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Please fix the issues above before running the analysis.")
        sys.exit(1)
    
    # Load and display configuration
    config = load_config()
    print_configuration(config)
    
    # Confirm before proceeding (skip in non-interactive mode)
    import sys
    if sys.stdin.isatty():
        print("📌 This will run the complete analysis pipeline.")
        response = input("   Continue? (y/n): ").strip().lower()
        if response != 'y':
            print("\n❌ Analysis cancelled by user.")
            sys.exit(0)
    else:
        print("📌 Running in non-interactive mode...")
    
    # Track results
    results = []
    total_start_time = time.time()
    
    # Run each script in the pipeline
    total_steps = len(PIPELINE_SCRIPTS)
    for i, step in enumerate(PIPELINE_SCRIPTS, 1):
        script_path = TEMPLATE_DIR / 'scripts' / step['script']
        
        # Print step header
        print_step_header(i, total_steps, step['name'], step['description'])
        
        # Check if script exists
        if not script_path.exists():
            print(f"⚠️  Script not found: {script_path}")
            if step['required']:
                print("   This is a required step. Stopping pipeline.")
                results.append({
                    'name': step['name'],
                    'success': False,
                    'duration': 0
                })
                break
            else:
                print("   Skipping optional step...")
                continue
        
        # Run the script
        success, duration = run_script(script_path, step['name'])
        
        results.append({
            'name': step['name'],
            'success': success,
            'duration': duration
        })
        
        # Stop if required step failed
        if not success and step['required']:
            print(f"\n❌ Required step '{step['name']}' failed. Stopping pipeline.")
            break
        
        # Brief pause between steps
        if i < total_steps:
            time.sleep(1)
    
    # Calculate total duration
    total_duration = time.time() - total_start_time
    
    # Print summary
    all_successful = print_summary(results, total_duration)
    
    # Exit with appropriate code
    sys.exit(0 if all_successful else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Analysis interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        logger.exception("Unexpected error in pipeline")
        sys.exit(1)