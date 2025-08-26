#!/usr/bin/env python3
"""
Consolidate NPIs from all 4 Corewell Health/Corewell CSV files
DA-175: Corewell Health Analysis - NPI Consolidation Script

This script:
1. Reads all 4 CSV files
2. Combines and deduplicates NPIs
3. Tracks source file for each provider
4. Cleans Medicare payment data
5. Saves consolidated dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
PROJECT_DIR = Path("/home/incent/conflixis-data-projects/projects/175-corewell-health-analysis")
DATA_DIR = PROJECT_DIR / "data" / "inputs"
OUTPUT_DIR = PROJECT_DIR / "rx-op-enhanced" / "data" / "processed"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def clean_currency_column(value):
    """Clean currency columns by removing $ and commas, converting to float"""
    if pd.isna(value) or value == '':
        return 0.0
    if isinstance(value, str):
        # Remove $, commas, and quotes
        cleaned = value.replace('$', '').replace(',', '').replace('"', '').strip()
        try:
            return float(cleaned) if cleaned else 0.0
        except ValueError:
            logger.warning(f"Could not convert value to float: {value}")
            return 0.0
    return float(value)

def clean_count_column(value):
    """Clean count columns by removing commas and converting to int"""
    if pd.isna(value) or value == '':
        return 0
    if isinstance(value, str):
        cleaned = value.replace(',', '').replace('"', '').strip()
        try:
            return int(float(cleaned)) if cleaned else 0
        except ValueError:
            logger.warning(f"Could not convert value to int: {value}")
            return 0
    return int(value)

def process_csv_file(filepath, source_name):
    """Process a single CSV file and return standardized dataframe"""
    logger.info(f"Processing {source_name}...")
    
    df = pd.read_csv(filepath)
    initial_count = len(df)
    
    # Add source file column
    df['source_file'] = source_name
    df['source_hospital_system'] = source_name.replace('Corewell_', '').replace('.csv', '')
    
    # Clean Medicare payment columns
    currency_columns = ['Medicare Pmts', 'Medicare Charges', 'Medicare Allowed Amt']
    for col in currency_columns:
        if col in df.columns:
            df[f'{col}_cleaned'] = df[col].apply(clean_currency_column)
    
    # Clean procedure count
    if '# of Medicare Procedures' in df.columns:
        df['Medicare_Procedures_cleaned'] = df['# of Medicare Procedures'].apply(clean_count_column)
    
    # Create full name column for easier identification
    df['Full_Name'] = df['Last Name'].fillna('') + ', ' + df['First Name'].fillna('')
    if 'Middle Name' in df.columns:
        df['Full_Name'] = df['Full_Name'] + ' ' + df['Middle Name'].fillna('')
    
    # Ensure NPI is string and clean
    df['NPI'] = df['NPI'].astype(str).str.strip()
    
    logger.info(f"  - Loaded {initial_count} providers from {source_name}")
    logger.info(f"  - Unique NPIs: {df['NPI'].nunique()}")
    
    return df

def main():
    """Main consolidation function"""
    logger.info("=" * 80)
    logger.info("Corewell Health/Corewell NPI Consolidation")
    logger.info("=" * 80)
    
    # Define CSV files to process
    csv_files = [
        ('Corewell_BHSH.csv', 'BHSH'),
        ('Corewell_Beaumont.csv', 'Beaumont'),
        ('Corewell_Health_South.csv', 'Health_South'),
        ('Corewell_Health_West.csv', 'Health_West')
    ]
    
    # Process all files
    all_dfs = []
    for filename, source in csv_files:
        filepath = DATA_DIR / filename
        if filepath.exists():
            df = process_csv_file(filepath, filename)
            all_dfs.append(df)
        else:
            logger.error(f"File not found: {filepath}")
    
    # Combine all dataframes
    logger.info("\nCombining all dataframes...")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Statistics before deduplication
    logger.info(f"\nCombined Statistics:")
    logger.info(f"  - Total rows: {len(combined_df)}")
    logger.info(f"  - Unique NPIs: {combined_df['NPI'].nunique()}")
    
    # Check for duplicate NPIs across files
    npi_counts = combined_df.groupby('NPI')['source_file'].nunique()
    multi_source_npis = npi_counts[npi_counts > 1]
    
    if len(multi_source_npis) > 0:
        logger.info(f"  - NPIs appearing in multiple files: {len(multi_source_npis)}")
        
        # Show examples of NPIs in multiple files
        logger.info("\nExamples of NPIs in multiple files:")
        for npi in list(multi_source_npis.index)[:5]:
            sources = combined_df[combined_df['NPI'] == npi]['source_file'].unique()
            logger.info(f"    NPI {npi}: {', '.join(sources)}")
    
    # Create deduplicated dataset (keeping first occurrence)
    deduplicated_df = combined_df.drop_duplicates(subset=['NPI'], keep='first')
    
    logger.info(f"\nAfter deduplication:")
    logger.info(f"  - Unique providers: {len(deduplicated_df)}")
    
    # Specialty distribution
    logger.info("\nSpecialty Distribution (Top 20):")
    specialty_counts = deduplicated_df['Primary Specialty'].value_counts().head(20)
    for specialty, count in specialty_counts.items():
        logger.info(f"  - {specialty}: {count}")
    
    # Source file distribution
    logger.info("\nSource File Distribution:")
    source_counts = combined_df.groupby('source_file')['NPI'].nunique()
    for source, count in source_counts.items():
        logger.info(f"  - {source}: {count} unique NPIs")
    
    # Medicare payment statistics
    if 'Medicare Pmts_cleaned' in deduplicated_df.columns:
        total_medicare_pmts = deduplicated_df['Medicare Pmts_cleaned'].sum()
        avg_medicare_pmts = deduplicated_df[deduplicated_df['Medicare Pmts_cleaned'] > 0]['Medicare Pmts_cleaned'].mean()
        
        logger.info(f"\nMedicare Payment Statistics:")
        logger.info(f"  - Total Medicare Payments: ${total_medicare_pmts:,.2f}")
        logger.info(f"  - Average Payment (non-zero): ${avg_medicare_pmts:,.2f}")
        logger.info(f"  - Providers with payments: {(deduplicated_df['Medicare Pmts_cleaned'] > 0).sum()}")
    
    # Save consolidated data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save full combined dataset (with duplicates)
    full_output_path = OUTPUT_DIR / f"corewell_health_all_providers_{timestamp}.csv"
    combined_df.to_csv(full_output_path, index=False)
    logger.info(f"\nSaved full dataset to: {full_output_path}")
    
    # Save deduplicated dataset
    dedup_output_path = OUTPUT_DIR / f"corewell_health_unique_providers_{timestamp}.csv"
    deduplicated_df.to_csv(dedup_output_path, index=False)
    logger.info(f"Saved deduplicated dataset to: {dedup_output_path}")
    
    # Save NPI list for BigQuery
    npi_list_path = OUTPUT_DIR / f"corewell_health_npi_list_{timestamp}.csv"
    npi_df = deduplicated_df[['NPI', 'Full_Name', 'Primary Specialty', 
                               'Primary Hospital Affiliation', 'source_hospital_system',
                               'HQ_STATE' if 'HQ_STATE' in deduplicated_df.columns else 'State']].copy()
    
    # Rename State to HQ_STATE for consistency
    if 'State' in npi_df.columns and 'HQ_STATE' not in npi_df.columns:
        npi_df.rename(columns={'State': 'HQ_STATE'}, inplace=True)
    
    npi_df.to_csv(npi_list_path, index=False)
    logger.info(f"Saved NPI list for BigQuery to: {npi_list_path}")
    
    # Create summary statistics file
    summary_path = OUTPUT_DIR / f"consolidation_summary_{timestamp}.txt"
    with open(summary_path, 'w') as f:
        f.write("Corewell Health/Corewell NPI Consolidation Summary\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("Files Processed:\n")
        for filename, _ in csv_files:
            f.write(f"  - {filename}\n")
        
        f.write(f"\nTotal Records: {len(combined_df)}\n")
        f.write(f"Unique NPIs: {combined_df['NPI'].nunique()}\n")
        f.write(f"Duplicate NPIs Removed: {len(combined_df) - len(deduplicated_df)}\n")
        
        if len(multi_source_npis) > 0:
            f.write(f"\nNPIs in Multiple Files: {len(multi_source_npis)}\n")
        
        f.write("\n" + "=" * 60 + "\n")
    
    logger.info(f"Saved summary to: {summary_path}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Consolidation Complete!")
    logger.info("=" * 80)
    
    return deduplicated_df

if __name__ == "__main__":
    consolidated_df = main()