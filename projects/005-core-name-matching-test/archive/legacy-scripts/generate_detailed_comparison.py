#!/usr/bin/env python3
"""
Generate detailed comparison between Tier 1 and Tier 2 test results
"""

import pandas as pd
import sys

def load_and_fix_results(filepath):
    """Load results and fix boolean/string type issues."""
    df = pd.read_csv(filepath)
    
    # Convert expected_match to boolean if it's string
    if df['expected_match'].dtype == object:
        df['expected_match'] = df['expected_match'].str.upper() == 'TRUE'
    
    # Convert predicted_match to boolean if it's string  
    if df['predicted_match'].dtype == object:
        df['predicted_match'] = df['predicted_match'].str.upper() == 'TRUE'
    
    # Recalculate correct column
    df['correct'] = df['expected_match'] == df['predicted_match']
    
    return df

def main():
    # Load both test results
    print("Loading test results...")
    tier1_df = load_and_fix_results('test-data/test-results/test_results_fuzzy_matching_85pct_threshold_latest.csv')
    tier2_df = load_and_fix_results('test-data/test-results/test_results_tier2_fuzzy_openai_90pct_latest.csv')
    
    # Take first 100 rows for comparison
    tier1_df = tier1_df.head(100)
    tier2_df = tier2_df.head(100)
    
    print(f"Tier 1 accuracy: {tier1_df['correct'].mean()*100:.1f}%")
    print(f"Tier 2 accuracy: {tier2_df['correct'].mean()*100:.1f}%")
    
    # Find improvements and regressions
    improvements = []
    regressions = []
    
    for i in range(len(tier1_df)):
        t1 = tier1_df.iloc[i]
        t2 = tier2_df.iloc[i]
        
        if not t1['correct'] and t2['correct']:
            # Tier 2 fixed a Tier 1 mistake
            improvements.append({
                'variant': t2['variant_name'],
                'reference': t2['reference_name'],
                'type': t2['variant_type'],
                'expected': t2['expected_match'],
                't1_pred': t1['predicted_match'],
                't1_score': t1['confidence_score'],
                't2_pred': t2['predicted_match'],
                't2_fuzzy': t2['tier1_score'],
                't2_openai': t2.get('tier2_score', None),
                't2_final': t2.get('aggregated_score', t2['confidence_score'])
            })
        elif t1['correct'] and not t2['correct']:
            # Tier 2 broke a Tier 1 success
            regressions.append({
                'variant': t2['variant_name'],
                'reference': t2['reference_name'],
                'type': t2['variant_type'],
                'expected': t2['expected_match'],
                't1_pred': t1['predicted_match'],
                't1_score': t1['confidence_score'],
                't2_pred': t2['predicted_match'],
                't2_fuzzy': t2['tier1_score'],
                't2_openai': t2.get('tier2_score', None),
                't2_final': t2.get('aggregated_score', t2['confidence_score'])
            })
    
    print(f"\n📈 Improvements: {len(improvements)} cases")
    print(f"📉 Regressions: {len(regressions)} cases")
    print(f"Net gain: +{len(improvements) - len(regressions)} cases")
    
    # Show detailed examples
    print("\n" + "="*80)
    print("SAMPLE IMPROVEMENTS (Tier 2 fixed Tier 1 mistakes)")
    print("="*80)
    
    for imp in improvements[:5]:
        print(f"\n✅ {imp['variant']} vs {imp['reference']}")
        print(f"   Type: {imp['type']}, Expected: {'Match' if imp['expected'] else 'No Match'}")
        print(f"   Tier 1: Predicted={'Match' if imp['t1_pred'] else 'No Match'}, Score={imp['t1_score']:.1f}% ❌")
        
        if pd.notna(imp['t2_openai']):
            print(f"   Tier 2: Fuzzy={imp['t2_fuzzy']:.1f}%, OpenAI={imp['t2_openai']:.0f}%, Final={imp['t2_final']:.1f}% ✅")
        else:
            print(f"   Tier 2: Score={imp['t2_final']:.1f}% (Matched at Tier 1 threshold) ✅")
    
    if regressions:
        print("\n" + "="*80)
        print("SAMPLE REGRESSIONS (Tier 2 broke Tier 1 successes)")
        print("="*80)
        
        for reg in regressions[:5]:
            print(f"\n❌ {reg['variant']} vs {reg['reference']}")
            print(f"   Type: {reg['type']}, Expected: {'Match' if reg['expected'] else 'No Match'}")
            print(f"   Tier 1: Predicted={'Match' if reg['t1_pred'] else 'No Match'}, Score={reg['t1_score']:.1f}% ✅")
            
            if pd.notna(reg['t2_openai']):
                print(f"   Tier 2: Fuzzy={reg['t2_fuzzy']:.1f}%, OpenAI={reg['t2_openai']:.0f}%, Final={reg['t2_final']:.1f}% ❌")
            else:
                print(f"   Tier 2: Score={reg['t2_final']:.1f}% ❌")
    
    # Analyze by variant type
    print("\n" + "="*80)
    print("PERFORMANCE BY VARIANT TYPE")
    print("="*80)
    
    variant_types = tier1_df['variant_type'].unique()
    
    comparison_data = []
    for vtype in variant_types:
        t1_type = tier1_df[tier1_df['variant_type'] == vtype]
        t2_type = tier2_df[tier2_df['variant_type'] == vtype]
        
        t1_acc = t1_type['correct'].mean() * 100
        t2_acc = t2_type['correct'].mean() * 100
        improvement = t2_acc - t1_acc
        
        comparison_data.append({
            'Type': vtype,
            'Tier 1': f"{t1_acc:.1f}%",
            'Tier 2': f"{t2_acc:.1f}%",
            'Change': f"{improvement:+.1f}%",
            'Status': '✅' if improvement > 0 else ('❌' if improvement < 0 else '➖')
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df = comparison_df.sort_values('Change', ascending=False)
    
    print("\n" + comparison_df.to_string(index=False))
    
    # Cases where OpenAI made the difference
    print("\n" + "="*80)
    print("OPENAI IMPACT ANALYSIS")
    print("="*80)
    
    openai_called = tier2_df[tier2_df['tier2_score'].notna()]
    print(f"\nTotal cases requiring OpenAI: {len(openai_called)} out of {len(tier2_df)}")
    
    # Find cases where OpenAI score > fuzzy score and led to correct match
    openai_wins = openai_called[
        (openai_called['tier2_score'] > openai_called['tier1_score']) &
        (openai_called['correct']) &
        (openai_called['expected_match'] == True)
    ]
    
    print(f"Cases where OpenAI score > Fuzzy and led to correct match: {len(openai_wins)}")
    
    if len(openai_wins) > 0:
        print("\nExamples where OpenAI understood the match better:")
        for i, row in openai_wins.head(3).iterrows():
            print(f"\n   {row['variant_name']} vs {row['reference_name']}")
            print(f"   Fuzzy: {row['tier1_score']:.1f}%, OpenAI: {row['tier2_score']:.0f}%")

if __name__ == "__main__":
    main()