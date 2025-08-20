#!/usr/bin/env python3
"""
Test Dataset Generator for Name Matching Evaluation
Generates realistic company name variants for testing matching algorithms
"""

import pandas as pd
import random
import re
from typing import List, Tuple
import os
import sys

random.seed(42)  # For reproducibility

class NameVariantGenerator:
    """Generate realistic name variants for testing."""
    
    def __init__(self, reference_file: str):
        """Load reference company names with IDs."""
        self.companies = []
        self.company_dict = {}  # Store ID -> name mapping
        with open(reference_file, 'r') as f:
            for line in f:
                if '|' in line:
                    id_str, name = line.strip().split('|', 1)
                    self.companies.append(name)
                    self.company_dict[name] = id_str
        print(f"Loaded {len(self.companies)} company names with IDs")
        
        # Common abbreviations in healthcare/pharma
        self.abbreviations = {
            'Corporation': ['Corp', 'Corp.', 'CORP'],
            'Incorporated': ['Inc', 'Inc.', 'INC'],
            'Limited': ['Ltd', 'Ltd.', 'LTD'],
            'Company': ['Co', 'Co.', 'CO'],
            'Medical': ['Med', 'Med.', 'MED'],
            'Pharmaceutical': ['Pharma', 'Pharm'],
            'Pharmaceuticals': ['Pharma', 'Pharm'],
            'Laboratory': ['Lab', 'Labs'],
            'Laboratories': ['Lab', 'Labs'],
            'Technologies': ['Tech', 'Technology'],
            'International': ['Intl', 'Int\'l', 'INT'],
            'Associates': ['Assoc', 'Assocs'],
            'Solutions': ['Sol', 'Soln'],
            'Systems': ['Sys', 'System'],
            'Manufacturing': ['Mfg', 'Mfg.'],
            'Distribution': ['Dist', 'Distr'],
            'Healthcare': ['Health Care', 'Health']
        }
        
    def create_abbreviation_variant(self, name: str) -> str:
        """Create variant with abbreviations."""
        variant = name
        for full, abbrevs in self.abbreviations.items():
            if full in variant:
                variant = variant.replace(full, random.choice(abbrevs))
                break
        return variant
    
    def create_punctuation_variant(self, name: str) -> str:
        """Create variant with punctuation changes."""
        variant = name
        # Remove or add periods after abbreviations
        if 'Inc.' in variant:
            variant = variant.replace('Inc.', 'Inc')
        elif 'Inc' in variant and 'Inc.' not in variant:
            variant = variant.replace('Inc', 'Inc.')
        
        # Handle LLC variations
        if 'LLC' in variant:
            variant = variant.replace('LLC', random.choice(['L.L.C.', 'L.L.C', 'llc']))
        
        # Handle commas
        if ', ' in variant:
            variant = variant.replace(', ', ' ')
        
        return variant
    
    def create_case_variant(self, name: str) -> str:
        """Create variant with case changes."""
        choice = random.choice(['upper', 'lower', 'title', 'mixed'])
        if choice == 'upper':
            return name.upper()
        elif choice == 'lower':
            return name.lower()
        elif choice == 'title':
            return name.title()
        else:  # mixed
            words = name.split()
            return ' '.join([w.upper() if random.random() > 0.5 else w.lower() 
                           for w in words])
    
    def create_typo_variant(self, name: str) -> str:
        """Create variant with common typos."""
        variant = name
        typo_type = random.choice(['swap', 'duplicate', 'missing'])
        
        # Only apply to alphanumeric characters
        chars = list(variant)
        alpha_indices = [i for i, c in enumerate(chars) if c.isalpha()]
        
        if len(alpha_indices) > 2:
            if typo_type == 'swap' and len(alpha_indices) > 1:
                # Swap adjacent characters
                idx = random.choice(alpha_indices[:-1])
                if chars[idx+1].isalpha():
                    chars[idx], chars[idx+1] = chars[idx+1], chars[idx]
            elif typo_type == 'duplicate':
                # Duplicate a character
                idx = random.choice(alpha_indices)
                chars[idx] = chars[idx] * 2
            elif typo_type == 'missing':
                # Remove a character (not first or last)
                if len(alpha_indices) > 2:
                    idx = random.choice(alpha_indices[1:-1])
                    chars[idx] = ''
        
        return ''.join(chars)
    
    def create_word_order_variant(self, name: str) -> str:
        """Create variant with reordered words (for multi-word names)."""
        # Only for names with "&" or "and"
        if ' & ' in name or ' and ' in name:
            parts = re.split(r' & | and ', name)
            if len(parts) == 2:
                connector = ' & ' if ' & ' in name else ' and '
                return parts[1] + connector + parts[0]
        return name
    
    def create_extra_words_variant(self, name: str) -> str:
        """Add or remove common words."""
        add_words = ['The', 'Global', 'USA', 'America', 'US', 'North American']
        remove_words = ['The', 'Inc', 'LLC', 'Corp', 'Company']
        
        if random.random() > 0.5:
            # Add a word
            word = random.choice(add_words)
            return f"{word} {name}"
        else:
            # Remove a word if present
            for word in remove_words:
                if word in name:
                    return name.replace(word, '').strip()
        return name
    
    def create_similar_but_different(self, name: str) -> Tuple[str, str]:
        """Create a similar but different company (false positive)."""
        # Get a different company with similar characteristics
        candidates = [c for c in self.companies if c != name and len(c) > 5]
        
        # Try to find companies with similar prefixes or industry terms
        words = name.split()
        if words:
            first_word = words[0]
            similar = [c for c in candidates if c.startswith(first_word[:3])]
            if similar:
                return name, random.choice(similar)
        
        # Otherwise, create a variant by changing key words
        variant = name
        replacements = {
            'Medical': 'Medicine',
            'Pharma': 'Pharmaceutical',
            'Tech': 'Technologies',
            'Systems': 'Solutions',
            'Group': 'Corporation',
            'Labs': 'Research',
            'Surgical': 'Surgery',
            'Devices': 'Equipment',
            'Diagnostics': 'Diagnostic'
        }
        
        for old, new in replacements.items():
            if old in variant:
                different_company = variant.replace(old, new)
                if different_company != variant:
                    return name, different_company
        
        # If no good replacement, return a random different company
        return name, random.choice(candidates)
    
    def generate_test_pairs(self, num_pairs: int = 1000) -> pd.DataFrame:
        """Generate test dataset with specified distribution."""
        test_data = []
        
        # Distribution
        n_true_positive = int(num_pairs * 0.4)
        n_false_positive = int(num_pairs * 0.3)
        n_true_negative = num_pairs - n_true_positive - n_false_positive
        
        print(f"Generating {n_true_positive} true positives...")
        # True Positives - Same company with variations
        for _ in range(n_true_positive):
            ref_name = random.choice(self.companies)
            ref_id = self.company_dict.get(ref_name, '')
            variant_type = random.choice([
                'abbreviation', 'punctuation', 'case', 'typo', 
                'word_order', 'extra_words', 'combined'
            ])
            
            if variant_type == 'abbreviation':
                variant = self.create_abbreviation_variant(ref_name)
            elif variant_type == 'punctuation':
                variant = self.create_punctuation_variant(ref_name)
            elif variant_type == 'case':
                variant = self.create_case_variant(ref_name)
            elif variant_type == 'typo':
                variant = self.create_typo_variant(ref_name)
            elif variant_type == 'word_order':
                variant = self.create_word_order_variant(ref_name)
            elif variant_type == 'extra_words':
                variant = self.create_extra_words_variant(ref_name)
            else:  # combined
                variant = ref_name
                for func in random.sample([
                    self.create_abbreviation_variant,
                    self.create_punctuation_variant,
                    self.create_case_variant
                ], 2):
                    variant = func(variant)
                variant_type = 'combined'
            
            test_data.append({
                'reference_id': ref_id,
                'reference_name': ref_name,
                'variant_name': variant,
                'expected_match': 'TRUE',
                'variant_type': variant_type
            })
        
        print(f"Generating {n_false_positive} false positives...")
        # False Positives - Similar but different companies
        used_pairs = set()
        for _ in range(n_false_positive):
            attempts = 0
            while attempts < 10:
                ref_name = random.choice(self.companies)
                ref_id = self.company_dict.get(ref_name, '')
                ref, similar = self.create_similar_but_different(ref_name)
                pair = (ref, similar)
                if pair not in used_pairs:
                    used_pairs.add(pair)
                    # For false positives, we use different ID or empty
                    similar_id = self.company_dict.get(similar, '')
                    # If it's the same ID, set to empty since these should be different companies
                    if similar_id == ref_id:
                        similar_id = ''
                    test_data.append({
                        'reference_id': ref_id,
                        'reference_name': ref,
                        'variant_name': similar,
                        'expected_match': 'FALSE',
                        'variant_type': 'similar_different'
                    })
                    break
                attempts += 1
        
        print(f"Generating {n_true_negative} true negatives...")
        # True Negatives - Completely different companies
        for _ in range(n_true_negative):
            companies_sample = random.sample(self.companies, 2)
            ref_id = self.company_dict.get(companies_sample[1], '')
            variant_id = self.company_dict.get(companies_sample[0], '')
            test_data.append({
                'reference_id': ref_id,
                'reference_name': companies_sample[1],
                'variant_name': companies_sample[0],
                'expected_match': 'FALSE',
                'variant_type': 'completely_different'
            })
        
        # Shuffle the data to mix different types
        random.shuffle(test_data)
        
        df = pd.DataFrame(test_data)
        return df
    
def main():
    """Generate test dataset."""
    # Path to reference file
    reference_file = '/home/incent/conflixis-data-projects/reference/op_suppliers.txt'
    
    if not os.path.exists(reference_file):
        print(f"Error: Reference file not found at {reference_file}")
        sys.exit(1)
    
    # Generate test dataset
    generator = NameVariantGenerator(reference_file)
    test_df = generator.generate_test_pairs(1000)
    
    # Save to CSV
    output_file = 'test-data/test-data-inputs/test_dataset.csv'
    test_df.to_csv(output_file, index=False)
    print(f"\nTest dataset saved to {output_file}")
    
    # Print summary statistics
    print("\nDataset Summary:")
    print(f"Total pairs: {len(test_df)}")
    print(f"True matches: {(test_df['expected_match'] == 'TRUE').sum()}")
    print(f"False matches: {(test_df['expected_match'] == 'FALSE').sum()}")
    print("\nVariant type distribution:")
    print(test_df['variant_type'].value_counts())
    
    # Show sample entries
    print("\nSample entries:")
    print(test_df.head(10).to_string())

if __name__ == "__main__":
    main()