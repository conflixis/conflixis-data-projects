#!/usr/bin/env python3
"""
Generate realistic sample disclosure data for demonstration
This simulates what would be pulled from BigQuery
"""

import json
import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STAGING_DIR = DATA_DIR / "staging"

# Ensure directories exist
STAGING_DIR.mkdir(parents=True, exist_ok=True)

def generate_realistic_disclosure_data(num_records=500):
    """Generate realistic healthcare provider disclosure data"""
    
    # Realistic provider names
    first_names = [
        'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
        'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
        'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
        'Matthew', 'Betty', 'Anthony', 'Dorothy', 'Kenneth', 'Sandra', 'Joshua', 'Ashley',
        'Kevin', 'Kimberly', 'Brian', 'Emily', 'George', 'Donna', 'Edward', 'Michelle'
    ]
    
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
        'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
        'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
        'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
        'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill'
    ]
    
    # Medical specialties
    specialties = [
        'Cardiology', 'Orthopedic Surgery', 'Neurology', 'Oncology', 'Radiology',
        'Internal Medicine', 'Family Medicine', 'Pediatrics', 'Emergency Medicine',
        'Anesthesiology', 'Psychiatry', 'Obstetrics and Gynecology', 'General Surgery',
        'Ophthalmology', 'Dermatology', 'Gastroenterology', 'Pulmonology', 'Nephrology',
        'Endocrinology', 'Rheumatology', 'Infectious Disease', 'Pathology', 'Urology'
    ]
    
    # Healthcare entities
    entities = [
        'Medtronic Inc', 'Johnson & Johnson', 'Abbott Laboratories', 'Stryker Corporation',
        'Boston Scientific', 'Pfizer Inc', 'Roche Pharmaceuticals', 'Novartis AG',
        'Bristol-Myers Squibb', 'Merck & Co', 'AstraZeneca', 'Eli Lilly',
        'Regional Medical Center', 'University Hospital System', 'St. Mary\'s Healthcare',
        'Memorial Health Network', 'Premier Medical Group', 'Advanced Surgical Associates',
        'Cardiac Care Specialists', 'Neurological Institute', 'Cancer Treatment Centers',
        'Precision Medicine Labs', 'BioTech Innovations', 'Medical Device Solutions'
    ]
    
    # Departments
    departments = [
        'Medicine', 'Surgery', 'Pediatrics', 'Emergency', 'ICU', 'Oncology',
        'Cardiology', 'Neurology', 'Radiology', 'Pathology', 'Anesthesia',
        'Psychiatry', 'Orthopedics', 'OB/GYN', 'Research', 'Administration'
    ]
    
    # Relationship types
    relationship_types = [
        'Consulting', 'Speaking Engagement', 'Advisory Board', 'Research Grant',
        'Clinical Trial', 'Board Member', 'Equity Interest', 'Royalties',
        'Educational Grant', 'Travel Sponsorship', 'Expert Witness', 'Medical Director'
    ]
    
    # Generate records
    records = []
    
    for i in range(num_records):
        # Generate provider info
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        provider_name = f"Dr. {first_name} {last_name}"
        provider_npi = f"1{random.randint(100000000, 999999999)}"
        
        # Determine financial amount with realistic distribution
        # Most are small amounts, fewer large amounts
        rand = random.random()
        if rand < 0.4:  # 40% small amounts
            financial_amount = random.randint(500, 5000)
        elif rand < 0.7:  # 30% moderate
            financial_amount = random.randint(5001, 25000)
        elif rand < 0.9:  # 20% high
            financial_amount = random.randint(25001, 100000)
        else:  # 10% critical
            financial_amount = random.randint(100001, 500000)
        
        # Determine risk tier based on amount
        if financial_amount <= 5000:
            risk_tier = 'low'
        elif financial_amount <= 25000:
            risk_tier = 'moderate'
        elif financial_amount <= 100000:
            risk_tier = 'high'
        else:
            risk_tier = 'critical'
        
        # Decision authority level correlates with specialty/seniority
        if random.random() < 0.1:
            decision_authority = 'executive'
        elif random.random() < 0.3:
            decision_authority = 'director'
        elif random.random() < 0.6:
            decision_authority = 'manager'
        else:
            decision_authority = 'staff'
        
        # Review status based on risk
        if risk_tier == 'critical':
            review_status = random.choice(['pending', 'in_review', 'requires_management'])
        elif risk_tier == 'high':
            review_status = random.choice(['pending', 'in_review', 'approved', 'requires_management'])
        else:
            review_status = random.choice(['pending', 'approved', 'approved', 'approved'])  # Most low risk are approved
        
        # Management plan required for high/critical
        management_plan_required = risk_tier in ['high', 'critical'] or review_status == 'requires_management'
        
        # Generate dates
        disclosure_date = datetime.now() - timedelta(days=random.randint(1, 365))
        last_review = disclosure_date + timedelta(days=random.randint(1, 30))
        next_review = last_review + timedelta(days=random.randint(30, 180))
        
        # Open Payments correlation (80% of disclosed amount typically)
        open_payments_total = int(financial_amount * random.uniform(0.7, 0.9))
        open_payments_matched = random.random() > 0.2  # 80% match rate
        
        record = {
            'id': i + 1,
            'provider_name': provider_name,
            'provider_npi': provider_npi,
            'specialty': random.choice(specialties),
            'department': random.choice(departments),
            'entity_name': random.choice(entities),
            'relationship_type': random.choice(relationship_types),
            'financial_amount': financial_amount,
            'open_payments_total': open_payments_total,
            'open_payments_matched': open_payments_matched,
            'open_payments_count': random.randint(1, 20),
            'risk_tier': risk_tier,
            'risk_score': random.randint(20, 95),
            'review_status': review_status,
            'management_plan_required': management_plan_required,
            'recusal_required': risk_tier == 'critical',
            'last_review_date': last_review.strftime('%Y-%m-%d'),
            'next_review_date': next_review.strftime('%Y-%m-%d'),
            'disclosure_date': disclosure_date.strftime('%Y-%m-%d'),
            'relationship_start_date': (disclosure_date - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d'),
            'relationship_ongoing': random.random() > 0.3,
            'decision_authority_level': decision_authority,
            'equity_percentage': random.uniform(0, 5) if random.random() > 0.8 else 0,
            'board_position': random.random() > 0.9,
            'created_at': disclosure_date.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': last_review.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        records.append(record)
    
    return pd.DataFrame(records)

def main():
    """Generate and save sample data"""
    print("Generating realistic sample disclosure data...")
    
    # Generate data
    df = generate_realistic_disclosure_data(500)
    
    # Calculate statistics
    stats = {
        'total_records': len(df),
        'risk_distribution': df['risk_tier'].value_counts().to_dict(),
        'review_status': df['review_status'].value_counts().to_dict(),
        'average_amount': float(df['financial_amount'].mean()),
        'median_amount': float(df['financial_amount'].median()),
        'max_amount': float(df['financial_amount'].max()),
        'management_plans_required': int(df['management_plan_required'].sum()),
        'op_match_rate': float(df['open_payments_matched'].mean() * 100)
    }
    
    print("\n=== Generated Data Statistics ===")
    print(f"Total Records: {stats['total_records']}")
    print(f"\nRisk Distribution:")
    for tier, count in stats['risk_distribution'].items():
        print(f"  {tier.capitalize()}: {count} ({count/stats['total_records']*100:.1f}%)")
    print(f"\nFinancial Amounts:")
    print(f"  Average: ${stats['average_amount']:,.2f}")
    print(f"  Median: ${stats['median_amount']:,.2f}")
    print(f"  Maximum: ${stats['max_amount']:,.2f}")
    print(f"\nCompliance Metrics:")
    print(f"  Management Plans Required: {stats['management_plans_required']}")
    print(f"  Open Payments Match Rate: {stats['op_match_rate']:.1f}%")
    
    # Save to staging directory as JSON for UI
    json_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'record_count': len(df),
            'data_range': f"{df['disclosure_date'].min()} to {df['disclosure_date'].max()}",
            'risk_distribution': stats['risk_distribution'],
            'source': 'Generated sample data (simulating BigQuery)'
        },
        'disclosures': df.to_dict('records')
    }
    
    json_path = STAGING_DIR / 'disclosure_data.json'
    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)
    
    print(f"\n✓ Data saved to: {json_path}")
    
    # Also save as CSV for reference
    csv_path = DATA_DIR / 'raw' / 'disclosures' / f'sample_disclosures_{datetime.now().strftime("%Y-%m-%d")}.csv'
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"✓ CSV backup saved to: {csv_path}")
    
    # Save as Parquet for performance
    parquet_path = STAGING_DIR / f'disclosures_ui_ready_{datetime.now().strftime("%Y-%m-%d")}.parquet'
    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
    print(f"✓ Parquet saved to: {parquet_path}")
    
    print(f"\n📊 Open the viewer to see the data:")
    print(f"   file:///home/incent/conflixis-data-projects/projects/011-disclosure-policy-review/disclosure-data-viewer.html")
    
    return df

if __name__ == "__main__":
    main()