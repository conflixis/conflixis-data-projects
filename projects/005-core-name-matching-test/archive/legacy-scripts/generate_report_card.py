#!/usr/bin/env python3
"""
Report Card Generator for Name Matching Tests
Generates comprehensive analysis and visualizations of test results
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import argparse
from typing import Dict, List


class ReportCardGenerator:
    """Generate comprehensive report cards for test results."""
    
    def __init__(self, results_file: str):
        """Load test results from CSV."""
        self.results_df = pd.read_csv(results_file)
        self.results_file = results_file
        
        # Ensure expected_match and predicted_match are strings for consistency
        if self.results_df['expected_match'].dtype == 'bool':
            self.results_df['expected_match'] = self.results_df['expected_match'].map({True: 'TRUE', False: 'FALSE'})
        if self.results_df['predicted_match'].dtype == 'bool':
            self.results_df['predicted_match'] = self.results_df['predicted_match'].map({True: 'TRUE', False: 'FALSE'})
            
    def calculate_metrics(self) -> Dict:
        """Calculate comprehensive performance metrics."""
        df = self.results_df
        
        # Basic counts
        total = len(df)
        
        # Confusion matrix components
        tp = ((df['expected_match'] == 'TRUE') & (df['predicted_match'] == 'TRUE')).sum()
        fp = ((df['expected_match'] == 'FALSE') & (df['predicted_match'] == 'TRUE')).sum()
        tn = ((df['expected_match'] == 'FALSE') & (df['predicted_match'] == 'FALSE')).sum()
        fn = ((df['expected_match'] == 'TRUE') & (df['predicted_match'] == 'FALSE')).sum()
        
        # Core metrics
        accuracy = ((tp + tn) / total) * 100 if total > 0 else 0
        precision = (tp / (tp + fp)) * 100 if (tp + fp) > 0 else 0
        recall = (tp / (tp + fn)) * 100 if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        # False positive/negative rates
        fpr = (fp / (fp + tn)) * 100 if (fp + tn) > 0 else 0
        fnr = (fn / (fn + tp)) * 100 if (fn + tp) > 0 else 0
        
        # Matthews Correlation Coefficient
        mcc_num = (tp * tn) - (fp * fn)
        mcc_den = np.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
        mcc = mcc_num / mcc_den if mcc_den > 0 else 0
        
        return {
            'total': total,
            'confusion_matrix': {
                'true_positive': tp,
                'false_positive': fp,
                'true_negative': tn,
                'false_negative': fn
            },
            'metrics': {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
                'false_positive_rate': fpr,
                'false_negative_rate': fnr,
                'matthews_correlation': mcc
            }
        }
    
    def analyze_by_variant_type(self) -> Dict:
        """Analyze performance by variant type."""
        variant_analysis = {}
        
        for vtype in self.results_df['variant_type'].unique():
            type_df = self.results_df[self.results_df['variant_type'] == vtype]
            
            tp = ((type_df['expected_match'] == 'TRUE') & 
                  (type_df['predicted_match'] == 'TRUE')).sum()
            fn = ((type_df['expected_match'] == 'TRUE') & 
                  (type_df['predicted_match'] == 'FALSE')).sum()
            fp = ((type_df['expected_match'] == 'FALSE') & 
                  (type_df['predicted_match'] == 'TRUE')).sum()
            tn = ((type_df['expected_match'] == 'FALSE') & 
                  (type_df['predicted_match'] == 'FALSE')).sum()
            
            accuracy = ((tp + tn) / len(type_df)) * 100 if len(type_df) > 0 else 0
            
            variant_analysis[vtype] = {
                'count': len(type_df),
                'accuracy': accuracy,
                'true_positive': tp,
                'false_positive': fp,
                'true_negative': tn,
                'false_negative': fn,
                'avg_confidence': type_df['confidence_score'].mean()
            }
        
        return variant_analysis
    
    def analyze_confidence_distribution(self) -> Dict:
        """Analyze confidence score distribution."""
        df = self.results_df
        
        # Correct predictions
        correct_df = df[df['correct'] == True]
        incorrect_df = df[df['correct'] == False]
        
        # True matches vs false matches
        true_matches = df[df['expected_match'] == 'TRUE']
        false_matches = df[df['expected_match'] == 'FALSE']
        
        return {
            'overall': {
                'mean': df['confidence_score'].mean(),
                'std': df['confidence_score'].std(),
                'min': df['confidence_score'].min(),
                'max': df['confidence_score'].max(),
                'median': df['confidence_score'].median()
            },
            'correct_predictions': {
                'mean': correct_df['confidence_score'].mean() if len(correct_df) > 0 else 0,
                'std': correct_df['confidence_score'].std() if len(correct_df) > 0 else 0,
                'count': len(correct_df)
            },
            'incorrect_predictions': {
                'mean': incorrect_df['confidence_score'].mean() if len(incorrect_df) > 0 else 0,
                'std': incorrect_df['confidence_score'].std() if len(incorrect_df) > 0 else 0,
                'count': len(incorrect_df)
            },
            'true_matches': {
                'mean': true_matches['confidence_score'].mean(),
                'predicted_correctly': ((true_matches['predicted_match'] == 'TRUE').sum() / len(true_matches)) * 100
            },
            'false_matches': {
                'mean': false_matches['confidence_score'].mean(),
                'predicted_correctly': ((false_matches['predicted_match'] == 'FALSE').sum() / len(false_matches)) * 100
            }
        }
    
    def find_edge_cases(self, n: int = 10) -> Dict:
        """Find interesting edge cases for review."""
        df = self.results_df
        
        # High confidence but wrong
        high_conf_wrong = df[(df['correct'] == False) & (df['confidence_score'] > 80)].nlargest(n, 'confidence_score')
        
        # Low confidence but correct
        low_conf_correct = df[(df['correct'] == True) & (df['confidence_score'] < 50)].nsmallest(n, 'confidence_score')
        
        # Borderline cases (around threshold)
        borderline = df[(df['confidence_score'] >= 83) & (df['confidence_score'] <= 87)]
        
        return {
            'high_confidence_errors': high_conf_wrong[['variant_name', 'reference_name', 'confidence_score', 'expected_match', 'predicted_match']].to_dict('records'),
            'low_confidence_correct': low_conf_correct[['variant_name', 'reference_name', 'confidence_score', 'expected_match', 'predicted_match']].to_dict('records'),
            'borderline_cases': borderline[['variant_name', 'reference_name', 'confidence_score', 'expected_match', 'predicted_match']].head(n).to_dict('records')
        }
    
    def generate_html_report(self, output_file: str):
        """Generate HTML report card."""
        metrics = self.calculate_metrics()
        variant_analysis = self.analyze_by_variant_type()
        confidence_dist = self.analyze_confidence_distribution()
        edge_cases = self.find_edge_cases()
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        algorithm = os.path.basename(self.results_file).split('_')[2]  # Extract algorithm name
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Name Matching Test Report - {algorithm.upper()}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .metric-card {{ background: white; padding: 20px; margin: 10px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .metric-box {{ background: #ecf0f1; padding: 15px; border-radius: 5px; text-align: center; }}
        .metric-value {{ font-size: 2em; font-weight: bold; color: #2c3e50; }}
        .metric-label {{ color: #7f8c8d; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #34495e; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .good {{ color: #27ae60; font-weight: bold; }}
        .bad {{ color: #e74c3c; font-weight: bold; }}
        .warning {{ color: #f39c12; font-weight: bold; }}
        .confusion-matrix {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; max-width: 400px; }}
        .cm-cell {{ padding: 10px; text-align: center; background: #ecf0f1; border-radius: 3px; }}
        .cm-header {{ font-weight: bold; background: #34495e; color: white; }}
        .edge-case {{ background: #fff3cd; padding: 10px; margin: 5px 0; border-radius: 3px; border-left: 4px solid #f39c12; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Name Matching Test Report Card</h1>
            <p>Algorithm: {algorithm.upper()} | Generated: {timestamp}</p>
            <p>Test File: {os.path.basename(self.results_file)}</p>
        </div>
        
        <div class="metric-card">
            <h2>Overall Performance</h2>
            <div class="metric-grid">
                <div class="metric-box">
                    <div class="metric-value {('good' if metrics['metrics']['accuracy'] > 90 else 'warning' if metrics['metrics']['accuracy'] > 80 else 'bad')}">{metrics['metrics']['accuracy']:.1f}%</div>
                    <div class="metric-label">Accuracy</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{metrics['metrics']['precision']:.1f}%</div>
                    <div class="metric-label">Precision</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{metrics['metrics']['recall']:.1f}%</div>
                    <div class="metric-label">Recall</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{metrics['metrics']['f1_score']:.1f}</div>
                    <div class="metric-label">F1 Score</div>
                </div>
            </div>
        </div>
        
        <div class="metric-card">
            <h2>Confusion Matrix</h2>
            <div style="display: flex; align-items: center; gap: 30px;">
                <div class="confusion-matrix">
                    <div class="cm-cell cm-header"></div>
                    <div class="cm-cell cm-header">Pred: TRUE</div>
                    <div class="cm-cell cm-header">Pred: FALSE</div>
                    <div class="cm-cell cm-header">Act: TRUE</div>
                    <div class="cm-cell" style="background: #d4edda;">{metrics['confusion_matrix']['true_positive']}</div>
                    <div class="cm-cell" style="background: #f8d7da;">{metrics['confusion_matrix']['false_negative']}</div>
                    <div class="cm-cell cm-header">Act: FALSE</div>
                    <div class="cm-cell" style="background: #f8d7da;">{metrics['confusion_matrix']['false_positive']}</div>
                    <div class="cm-cell" style="background: #d4edda;">{metrics['confusion_matrix']['true_negative']}</div>
                </div>
                <div>
                    <p><strong>False Positive Rate:</strong> {metrics['metrics']['false_positive_rate']:.1f}%</p>
                    <p><strong>False Negative Rate:</strong> {metrics['metrics']['false_negative_rate']:.1f}%</p>
                    <p><strong>Matthews Correlation:</strong> {metrics['metrics']['matthews_correlation']:.3f}</p>
                </div>
            </div>
        </div>
        
        <div class="metric-card">
            <h2>Performance by Variant Type</h2>
            <table>
                <tr>
                    <th>Variant Type</th>
                    <th>Count</th>
                    <th>Accuracy</th>
                    <th>Avg Confidence</th>
                    <th>TP</th>
                    <th>FP</th>
                    <th>TN</th>
                    <th>FN</th>
                </tr>
"""
        
        for vtype, stats in sorted(variant_analysis.items(), key=lambda x: x[1]['accuracy'], reverse=True):
            accuracy_class = 'good' if stats['accuracy'] > 90 else 'warning' if stats['accuracy'] > 70 else 'bad'
            html_content += f"""
                <tr>
                    <td>{vtype}</td>
                    <td>{stats['count']}</td>
                    <td class="{accuracy_class}">{stats['accuracy']:.1f}%</td>
                    <td>{stats['avg_confidence']:.1f}</td>
                    <td>{stats['true_positive']}</td>
                    <td>{stats['false_positive']}</td>
                    <td>{stats['true_negative']}</td>
                    <td>{stats['false_negative']}</td>
                </tr>
"""
        
        html_content += """
            </table>
        </div>
        
        <div class="metric-card">
            <h2>Confidence Score Analysis</h2>
            <div class="metric-grid">
                <div class="metric-box">
                    <div class="metric-value">{:.1f}</div>
                    <div class="metric-label">Mean (Correct)</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{:.1f}</div>
                    <div class="metric-label">Mean (Incorrect)</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{:.1f}%</div>
                    <div class="metric-label">True Matches Caught</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">{:.1f}%</div>
                    <div class="metric-label">False Matches Rejected</div>
                </div>
            </div>
        </div>
""".format(
            confidence_dist['correct_predictions']['mean'],
            confidence_dist['incorrect_predictions']['mean'],
            confidence_dist['true_matches']['predicted_correctly'],
            confidence_dist['false_matches']['predicted_correctly']
        )
        
        # Add edge cases section
        html_content += """
        <div class="metric-card">
            <h2>Edge Cases for Review</h2>
            <h3>High Confidence Errors (Should Review Algorithm)</h3>
"""
        
        for case in edge_cases['high_confidence_errors'][:5]:
            html_content += f"""
            <div class="edge-case">
                <strong>{case['variant_name']}</strong> vs <strong>{case['reference_name']}</strong><br>
                Confidence: {case['confidence_score']:.1f} | Expected: {case['expected_match']} | Predicted: {case['predicted_match']}
            </div>
"""
        
        html_content += """
            <h3>Low Confidence but Correct (Potential for Improvement)</h3>
"""
        
        for case in edge_cases['low_confidence_correct'][:5]:
            html_content += f"""
            <div class="edge-case">
                <strong>{case['variant_name']}</strong> vs <strong>{case['reference_name']}</strong><br>
                Confidence: {case['confidence_score']:.1f} | Expected: {case['expected_match']} | Predicted: {case['predicted_match']}
            </div>
"""
        
        html_content += """
        </div>
    </div>
</body>
</html>
"""
        
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        print(f"HTML report saved to {output_file}")
    
    def generate_csv_summary(self, output_file: str):
        """Generate CSV summary of metrics."""
        metrics = self.calculate_metrics()
        variant_analysis = self.analyze_by_variant_type()
        
        summary_data = []
        
        # Overall metrics
        summary_data.append({
            'category': 'Overall',
            'metric': 'Accuracy',
            'value': metrics['metrics']['accuracy']
        })
        summary_data.append({
            'category': 'Overall',
            'metric': 'Precision',
            'value': metrics['metrics']['precision']
        })
        summary_data.append({
            'category': 'Overall',
            'metric': 'Recall',
            'value': metrics['metrics']['recall']
        })
        summary_data.append({
            'category': 'Overall',
            'metric': 'F1 Score',
            'value': metrics['metrics']['f1_score']
        })
        
        # Variant type metrics
        for vtype, stats in variant_analysis.items():
            summary_data.append({
                'category': f'Variant_{vtype}',
                'metric': 'Accuracy',
                'value': stats['accuracy']
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(output_file, index=False)
        print(f"CSV summary saved to {output_file}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Generate report card for test results')
    parser.add_argument('--results-file', required=True,
                       help='Path to test results CSV file')
    parser.add_argument('--output-dir', default='test-data/reports',
                       help='Directory to save reports')
    parser.add_argument('--format', default='both',
                       choices=['html', 'csv', 'both'],
                       help='Report format')
    
    args = parser.parse_args()
    
    # Create output directory if needed
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate report
    generator = ReportCardGenerator(args.results_file)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = os.path.basename(args.results_file).replace('.csv', '')
    
    if args.format in ['html', 'both']:
        html_file = os.path.join(args.output_dir, f'report_{base_name}_{timestamp}.html')
        generator.generate_html_report(html_file)
    
    if args.format in ['csv', 'both']:
        csv_file = os.path.join(args.output_dir, f'summary_{base_name}_{timestamp}.csv')
        generator.generate_csv_summary(csv_file)


if __name__ == "__main__":
    main()