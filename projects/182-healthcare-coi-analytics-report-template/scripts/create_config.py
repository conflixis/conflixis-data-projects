#!/usr/bin/env python3
"""
Configuration Generator for Healthcare COI Analytics
Creates new health system configuration files from template
"""

import argparse
import sys
from pathlib import Path
import yaml
import shutil
from typing import Dict, Any

def load_template() -> Dict[str, Any]:
    """Load the template configuration file"""
    template_path = Path("config/template.yaml")
    if not template_path.exists():
        print(f"Error: Template file not found at {template_path}")
        sys.exit(1)
    
    with open(template_path, 'r') as f:
        return yaml.safe_load(f)

def create_config(name: str, short_name: str, npi_file: str, output_dir: str = "config") -> str:
    """
    Create a new configuration file for a health system
    
    Args:
        name: Full health system name
        short_name: Short abbreviation for file naming
        npi_file: Path to NPI CSV file
        output_dir: Directory to save config file
        
    Returns:
        Path to created config file
    """
    # Load template
    config = load_template()
    
    # Update health system specific settings
    config['health_system']['name'] = name
    config['health_system']['short_name'] = short_name
    config['health_system']['npi_file'] = npi_file
    
    # Update BigQuery table name to match short_name
    config['bigquery']['tables']['provider_npis'] = f"{short_name}_provider_npis"
    
    # Create output path
    output_path = Path(output_dir) / f"{short_name}.yaml"
    
    # Write config file
    with open(output_path, 'w') as f:
        # Add header comment
        f.write(f"# Healthcare COI Analytics Configuration - {name}\n")
        f.write(f"# Configuration for {name} analysis\n\n")
        
        # Write YAML content
        yaml.dump(config, f, 
                  default_flow_style=False, 
                  sort_keys=False,
                  allow_unicode=True)
    
    return str(output_path)

def validate_npi_file(npi_file: str) -> bool:
    """Validate that NPI file exists and has required columns"""
    npi_path = Path(npi_file)
    
    if not npi_path.exists():
        print(f"Warning: NPI file not found at {npi_file}")
        print("Make sure to add the file before running analysis")
        return False
    
    try:
        import pandas as pd
        df = pd.read_csv(npi_path, nrows=5)
        required_cols = ['NPI']
        
        for col in required_cols:
            if col not in df.columns:
                print(f"Warning: Required column '{col}' not found in NPI file")
                return False
        
        print(f"✓ NPI file validated: {len(df)} sample rows loaded")
        return True
        
    except Exception as e:
        print(f"Warning: Could not validate NPI file: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Create configuration file for a new health system',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/create_config.py --name "Cleveland Clinic" --short cleveland --npi data/inputs/cleveland-npis.csv
  python scripts/create_config.py --name "Mayo Clinic" --short mayo --npi data/inputs/mayo-npis.csv --validate
  
The script will:
  1. Create a new config file in config/<short_name>.yaml
  2. Update health system name and NPI file path
  3. Set BigQuery table names appropriately
  4. Optionally validate the NPI file exists and has correct format
        """
    )
    
    parser.add_argument('--name', 
                       required=True,
                       help='Full health system name (e.g., "Cleveland Clinic")')
    
    parser.add_argument('--short', 
                       required=True,
                       help='Short abbreviation for file naming (e.g., "cleveland")')
    
    parser.add_argument('--npi', 
                       required=True,
                       help='Path to NPI CSV file (e.g., "data/inputs/cleveland-npis.csv")')
    
    parser.add_argument('--output-dir',
                       default='config',
                       help='Output directory for config file (default: config)')
    
    parser.add_argument('--validate',
                       action='store_true',
                       help='Validate NPI file exists and has correct format')
    
    parser.add_argument('--force',
                       action='store_true',
                       help='Overwrite existing config file if it exists')
    
    args = parser.parse_args()
    
    # Check if config already exists
    output_path = Path(args.output_dir) / f"{args.short}.yaml"
    if output_path.exists() and not args.force:
        print(f"Error: Config file already exists at {output_path}")
        print("Use --force to overwrite")
        sys.exit(1)
    
    # Validate NPI file if requested
    if args.validate:
        validate_npi_file(args.npi)
    
    # Create configuration
    config_path = create_config(
        name=args.name,
        short_name=args.short,
        npi_file=args.npi,
        output_dir=args.output_dir
    )
    
    print(f"\n✅ Configuration created successfully!")
    print(f"   File: {config_path}")
    print(f"\nTo run analysis:")
    print(f"   python cli.py analyze --config {config_path} --force-reload")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())