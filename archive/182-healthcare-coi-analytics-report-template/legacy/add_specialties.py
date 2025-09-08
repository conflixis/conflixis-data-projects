#!/usr/bin/env python3
"""
Quick script to add random specialties to NPI file for testing
"""

import pandas as pd
import random
from pathlib import Path

# Specialties to randomly assign
SPECIALTIES = [
    'Internal Medicine',
    'Family Practice', 
    'Cardiology',
    'Oncology',
    'Orthopedic Surgery',
    'Neurology',
    'Psychiatry',
    'Emergency Medicine',
    'Anesthesiology',
    'Radiology',
    'Physician Assistant',
    'Nurse Practitioner',
    'Pediatrics',
    'Obstetrics & Gynecology',
    'General Surgery'
]

# Read the file
file_path = Path(__file__).parent.parent / 'data' / 'inputs' / 'provider_npis.csv'
df = pd.read_csv(file_path)

# Add random specialties
df['Primary_Specialty'] = [random.choice(SPECIALTIES) for _ in range(len(df))]

# Save back
df.to_csv(file_path, index=False)
print(f"Added specialties to {len(df)} providers")
print(df.head(10))