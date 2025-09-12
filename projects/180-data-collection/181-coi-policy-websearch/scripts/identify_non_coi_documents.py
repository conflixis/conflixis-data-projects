#!/usr/bin/env python3
"""
Identify and categorize documents as COI policies vs other document types
"""

import os
import json
from pathlib import Path
from datetime import datetime
import shutil

def analyze_documents():
    """Analyze all documents and categorize them"""
    
    base_dir = Path("data/raw/policies_categorized")
    
    # Categories of non-COI documents based on filename patterns
    non_coi_patterns = {
        'codes_of_conduct': [
            'Code_of_Conduct', 'CodeofConduct', 'Code of Conduct'
        ],
        'training_materials': [
            'Tutorial', 'User_Guide', 'User Guide', 'Training', 'Fact_Sheet', 
            'Fact Sheet', 'Guide', 'Handbook', 'Manual'
        ],
        'forms_and_disclosures': [
            'Form_700', 'Form 700', 'Disclosure', 'Statement', 'Attestation',
            'Declaration', 'Questionnaire'
        ],
        'booklets_and_brochures': [
            'Booklet', 'Brochure', 'Pamphlet', 'Summary'
        ],
        'reports_and_analyses': [
            'Report', 'Analysis', 'Review', 'Assessment'
        ],
        'agreements_and_contracts': [
            'Agreement', 'Contract', 'Terms', 'MOU'
        ],
        'newsletters_and_communications': [
            'Newsletter', 'Bulletin', 'Update', 'Announcement'
        ]
    }
    
    # Patterns that indicate actual COI policies
    coi_policy_patterns = [
        'COI_Policy', 'COI Policy', 'Conflict_of_Interest_Policy',
        'Conflict of Interest Policy', 'Institutional_COI', 'Research_COI',
        'Financial_Conflict', 'FCOI_Policy'
    ]
    
    results = {
        'actual_coi_policies': [],
        'non_coi_documents': [],
        'uncertain': []
    }
    
    # Scan all files
    for file_path in base_dir.rglob('*'):
        if file_path.is_file() and file_path.name != '.gitkeep':
            file_info = {
                'path': str(file_path.relative_to(base_dir)),
                'name': file_path.name,
                'category': file_path.parent.name,
                'size': file_path.stat().st_size
            }
            
            # Check if it's clearly NOT a COI policy
            is_non_coi = False
            non_coi_type = None
            for doc_type, patterns in non_coi_patterns.items():
                if any(pattern.lower() in file_path.name.lower() for pattern in patterns):
                    is_non_coi = True
                    non_coi_type = doc_type
                    break
            
            # Check if it's clearly a COI policy
            is_coi_policy = any(pattern.lower() in file_path.name.lower() 
                               for pattern in coi_policy_patterns)
            
            # Special cases - even with COI in name, these are not policies
            if 'Tutorial' in file_path.name or 'Guide' in file_path.name or 'Fact_Sheet' in file_path.name:
                is_non_coi = True
                is_coi_policy = False
                non_coi_type = 'training_materials'
            
            if 'Form' in file_path.name or 'Disclosure' in file_path.name or 'Statement' in file_path.name:
                if 'Policy' not in file_path.name:
                    is_non_coi = True
                    is_coi_policy = False
                    non_coi_type = 'forms_and_disclosures'
            
            # Categorize
            if is_non_coi:
                file_info['document_type'] = non_coi_type
                results['non_coi_documents'].append(file_info)
            elif is_coi_policy or 'COI' in file_path.name or 'Conflict' in file_path.name:
                results['actual_coi_policies'].append(file_info)
            else:
                # For files without clear indicators, assume they might be COI policies
                # (many organizations don't put "COI" in the filename)
                results['uncertain'].append(file_info)
    
    return results

def move_non_coi_documents(results):
    """Move non-COI documents to a separate folder"""
    
    base_dir = Path("data/raw/policies_categorized")
    non_coi_dir = Path("data/raw/non_coi_documents")
    
    # Create directories for non-COI documents
    for doc_type in ['codes_of_conduct', 'training_materials', 'forms_and_disclosures', 
                     'booklets_and_brochures', 'reports_and_analyses', 
                     'agreements_and_contracts', 'newsletters_and_communications']:
        (non_coi_dir / doc_type).mkdir(parents=True, exist_ok=True)
    
    moved_files = []
    
    for doc in results['non_coi_documents']:
        src = base_dir / doc['path']
        doc_type = doc.get('document_type', 'other')
        dst_dir = non_coi_dir / doc_type
        dst = dst_dir / doc['name']
        
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({
                'file': doc['name'],
                'from': str(src.parent.relative_to(base_dir)),
                'to': str(dst_dir.relative_to(Path("data/raw"))),
                'reason': f"Identified as {doc_type.replace('_', ' ')}"
            })
            print(f"Moved: {doc['name']} -> non_coi_documents/{doc_type}/")
    
    return moved_files

def save_report(results, moved_files):
    """Save detailed report of the analysis and cleanup"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/coi_cleanup_report_{timestamp}.json"
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_files_analyzed': (len(results['actual_coi_policies']) + 
                                    len(results['non_coi_documents']) + 
                                    len(results['uncertain'])),
            'actual_coi_policies': len(results['actual_coi_policies']),
            'non_coi_documents': len(results['non_coi_documents']),
            'uncertain': len(results['uncertain']),
            'files_moved': len(moved_files)
        },
        'actual_coi_policies': sorted(results['actual_coi_policies'], 
                                     key=lambda x: x['path']),
        'non_coi_documents': sorted(results['non_coi_documents'], 
                                   key=lambda x: x.get('document_type', '')),
        'uncertain_documents': sorted(results['uncertain'], 
                                     key=lambda x: x['path']),
        'moved_files': moved_files
    }
    
    os.makedirs("data/output", exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    return report_file, report

def print_summary(report):
    """Print summary of the cleanup"""
    
    print("\n" + "="*60)
    print("COI DOCUMENT CLEANUP SUMMARY")
    print("="*60)
    
    summary = report['summary']
    print(f"\nTotal files analyzed: {summary['total_files_analyzed']}")
    print(f"Actual COI policies: {summary['actual_coi_policies']}")
    print(f"Non-COI documents: {summary['non_coi_documents']}")
    print(f"Uncertain (kept as COI): {summary['uncertain']}")
    print(f"Files moved: {summary['files_moved']}")
    
    if report['non_coi_documents']:
        print("\n" + "-"*60)
        print("NON-COI DOCUMENTS IDENTIFIED:")
        print("-"*60)
        
        # Group by type
        by_type = {}
        for doc in report['non_coi_documents']:
            doc_type = doc.get('document_type', 'other')
            if doc_type not in by_type:
                by_type[doc_type] = []
            by_type[doc_type].append(doc['name'])
        
        for doc_type, files in sorted(by_type.items()):
            print(f"\n{doc_type.replace('_', ' ').title()}:")
            for file in files[:5]:  # Show first 5
                print(f"  - {file}")
            if len(files) > 5:
                print(f"  ... and {len(files) - 5} more")

def main():
    """Main execution"""
    
    print("Analyzing documents to identify non-COI files...")
    results = analyze_documents()
    
    print(f"\nFound {len(results['non_coi_documents'])} non-COI documents")
    print(f"Found {len(results['actual_coi_policies'])} actual COI policies")
    print(f"Found {len(results['uncertain'])} uncertain documents (will keep as COI)")
    
    if results['non_coi_documents']:
        print("\nMoving non-COI documents...")
        moved_files = move_non_coi_documents(results)
    else:
        moved_files = []
    
    print("\nSaving report...")
    report_file, report = save_report(results, moved_files)
    
    print_summary(report)
    print(f"\nDetailed report saved to: {report_file}")

if __name__ == "__main__":
    main()