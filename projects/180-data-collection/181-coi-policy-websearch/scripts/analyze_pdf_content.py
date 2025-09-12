#!/usr/bin/env python3
"""
Analyze PDF content to identify actual COI policies vs other documents
Also identifies dead/corrupted PDFs
"""

import os
import json
from pathlib import Path
from datetime import datetime
import PyPDF2
import pdfplumber

def extract_pdf_text(file_path, max_pages=5):
    """Extract text from first few pages of PDF to analyze content"""
    text = ""
    
    try:
        # Try with pdfplumber first (better text extraction)
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages[:max_pages]):
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
                    
        # If no text extracted, try PyPDF2
        if not text.strip():
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                num_pages = min(len(pdf_reader.pages), max_pages)
                
                for page_num in range(num_pages):
                    page = pdf_reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                        
    except Exception as e:
        return None, str(e)
    
    return text[:10000], None  # Return first 10k characters

def analyze_document_type(text, filename):
    """Analyze text content to determine document type"""
    
    if not text:
        return 'dead_or_corrupted', 0
    
    text_lower = text.lower()
    
    # Check file size issue (very small text often means extraction failed)
    if len(text.strip()) < 100:
        return 'likely_dead', 0
    
    # COI policy indicators (weighted scoring)
    coi_score = 0
    coi_keywords = {
        'conflict of interest policy': 10,
        'conflict of interest': 8,
        'conflicts of interest': 8,
        'financial conflict': 7,
        'institutional conflict': 7,
        'coi policy': 10,
        'fcoi policy': 9,
        'disclosure of interest': 6,
        'financial disclosure policy': 8,
        'outside activities': 5,
        'financial interests': 5,
        'personal financial interest': 6,
        'conflict management': 6,
        'conflict resolution': 5,
        'recusal': 4,
        'management plan': 4,
        'disclosure requirements': 5,
        'prohibited interests': 5,
        'appearance of conflict': 4
    }
    
    for keyword, weight in coi_keywords.items():
        if keyword in text_lower:
            coi_score += weight
    
    # Non-COI document indicators
    non_coi_indicators = {
        'code_of_conduct': [
            'code of conduct', 'code of ethics', 'ethical conduct',
            'standards of conduct', 'business conduct', 'professional conduct'
        ],
        'privacy_policy': [
            'privacy policy', 'privacy notice', 'hipaa', 'protected health information',
            'privacy practices', 'confidentiality policy'
        ],
        'compliance_program': [
            'compliance program', 'compliance plan', 'compliance and ethics program',
            'compliance manual', 'compliance guide'
        ],
        'training_material': [
            'training guide', 'training manual', 'tutorial', 'user guide',
            'how to complete', 'instructions for', 'step by step'
        ],
        'form_or_disclosure': [
            'disclosure form', 'disclosure statement', 'complete this form',
            'sign and date', 'print name', 'signature required', 'form 700'
        ],
        'vendor_policy': [
            'vendor policy', 'vendor code', 'supplier code', 'vendor requirements',
            'procurement policy', 'purchasing policy'
        ],
        'research_policy': [
            'research integrity', 'research misconduct', 'research ethics',
            'scientific integrity', 'responsible conduct of research'
        ],
        'hr_policy': [
            'employee handbook', 'human resources policy', 'employment policy',
            'workplace policy', 'personnel policy'
        ]
    }
    
    detected_type = None
    max_matches = 0
    
    for doc_type, indicators in non_coi_indicators.items():
        matches = sum(1 for indicator in indicators if indicator in text_lower)
        if matches > max_matches:
            max_matches = matches
            detected_type = doc_type
    
    # Decision logic
    if coi_score >= 20:
        return 'coi_policy', coi_score
    elif detected_type and max_matches >= 3:
        return detected_type, -max_matches
    elif coi_score >= 10:
        return 'likely_coi_policy', coi_score
    elif detected_type and max_matches >= 2:
        return detected_type, -max_matches
    elif 'conflict' in text_lower or 'coi' in text_lower:
        return 'possible_coi_policy', coi_score
    else:
        return 'unclear', 0

def analyze_all_documents():
    """Analyze all documents in the collection"""
    
    base_dir = Path("data/raw/policies_categorized")
    results = {
        'coi_policies': [],
        'likely_coi_policies': [],
        'possible_coi_policies': [],
        'non_coi_documents': {},
        'dead_or_corrupted': [],
        'unclear': [],
        'errors': []
    }
    
    total_files = 0
    
    for file_path in base_dir.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in ['.pdf', '.html', '.doc']:
            total_files += 1
            file_info = {
                'path': str(file_path.relative_to(base_dir)),
                'name': file_path.name,
                'category': file_path.parent.name,
                'size_kb': file_path.stat().st_size / 1024
            }
            
            print(f"Analyzing: {file_info['name'][:50]}...", end=' ')
            
            # Skip HTML and DOC files for now (focus on PDFs)
            if file_path.suffix.lower() in ['.html', '.doc']:
                file_info['reason'] = 'Non-PDF format - manual review needed'
                results['unclear'].append(file_info)
                print("SKIPPED (non-PDF)")
                continue
            
            # Extract text
            text, error = extract_pdf_text(file_path)
            
            if error:
                file_info['error'] = error
                results['errors'].append(file_info)
                print(f"ERROR: {error[:30]}")
                continue
            
            # Analyze content
            doc_type, score = analyze_document_type(text, file_path.name)
            file_info['detected_type'] = doc_type
            file_info['confidence_score'] = abs(score)
            
            # Add first 500 chars of text for review
            if text:
                file_info['text_preview'] = text[:500].replace('\n', ' ').strip()
            
            # Categorize
            if doc_type == 'coi_policy':
                results['coi_policies'].append(file_info)
                print("✓ COI Policy")
            elif doc_type == 'likely_coi_policy':
                results['likely_coi_policies'].append(file_info)
                print("~ Likely COI")
            elif doc_type == 'possible_coi_policy':
                results['possible_coi_policies'].append(file_info)
                print("? Possible COI")
            elif doc_type in ['dead_or_corrupted', 'likely_dead']:
                results['dead_or_corrupted'].append(file_info)
                print("✗ DEAD/CORRUPTED")
            elif doc_type == 'unclear':
                results['unclear'].append(file_info)
                print("? Unclear")
            else:
                if doc_type not in results['non_coi_documents']:
                    results['non_coi_documents'][doc_type] = []
                results['non_coi_documents'][doc_type].append(file_info)
                print(f"✗ {doc_type.replace('_', ' ').title()}")
    
    print(f"\n\nAnalyzed {total_files} files total")
    return results

def save_analysis_report(results):
    """Save detailed analysis report"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/pdf_content_analysis_{timestamp}.json"
    
    # Calculate summary
    summary = {
        'total_analyzed': sum([
            len(results['coi_policies']),
            len(results['likely_coi_policies']),
            len(results['possible_coi_policies']),
            sum(len(docs) for docs in results['non_coi_documents'].values()),
            len(results['dead_or_corrupted']),
            len(results['unclear']),
            len(results['errors'])
        ]),
        'confirmed_coi': len(results['coi_policies']),
        'likely_coi': len(results['likely_coi_policies']),
        'possible_coi': len(results['possible_coi_policies']),
        'non_coi': sum(len(docs) for docs in results['non_coi_documents'].values()),
        'dead_files': len(results['dead_or_corrupted']),
        'unclear': len(results['unclear']),
        'errors': len(results['errors'])
    }
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': summary,
        'detailed_results': results
    }
    
    os.makedirs("data/output", exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    return report_file, summary

def print_summary(summary, results):
    """Print analysis summary"""
    
    print("\n" + "="*60)
    print("PDF CONTENT ANALYSIS SUMMARY")
    print("="*60)
    
    print(f"\nTotal files analyzed: {summary['total_analyzed']}")
    print(f"Confirmed COI policies: {summary['confirmed_coi']}")
    print(f"Likely COI policies: {summary['likely_coi']}")
    print(f"Possible COI policies: {summary['possible_coi']}")
    print(f"Non-COI documents: {summary['non_coi']}")
    print(f"Dead/corrupted files: {summary['dead_files']}")
    print(f"Unclear: {summary['unclear']}")
    print(f"Errors: {summary['errors']}")
    
    if results['non_coi_documents']:
        print("\n" + "-"*60)
        print("NON-COI DOCUMENTS BY TYPE:")
        print("-"*60)
        for doc_type, docs in results['non_coi_documents'].items():
            print(f"\n{doc_type.replace('_', ' ').title()}: ({len(docs)} files)")
            for doc in docs[:3]:
                print(f"  - {doc['name']}")
            if len(docs) > 3:
                print(f"  ... and {len(docs) - 3} more")
    
    if results['dead_or_corrupted']:
        print("\n" + "-"*60)
        print("DEAD/CORRUPTED FILES:")
        print("-"*60)
        for doc in results['dead_or_corrupted'][:10]:
            print(f"  - {doc['name']} ({doc['size_kb']:.1f} KB)")
        if len(results['dead_or_corrupted']) > 10:
            print(f"  ... and {len(results['dead_or_corrupted']) - 10} more")

def main():
    """Main execution"""
    
    print("Starting comprehensive PDF content analysis...")
    print("This may take a few minutes...\n")
    
    results = analyze_all_documents()
    report_file, summary = save_analysis_report(results)
    
    print_summary(summary, results)
    print(f"\n\nDetailed report saved to: {report_file}")
    
    # Recommendation
    print("\n" + "="*60)
    print("RECOMMENDATION:")
    print("="*60)
    total_keep = summary['confirmed_coi'] + summary['likely_coi'] + summary['possible_coi']
    print(f"Keep as COI policies: {total_keep} files")
    print(f"Move/remove non-COI: {summary['non_coi']} files")
    print(f"Remove dead files: {summary['dead_files']} files")
    print(f"Manual review needed: {summary['unclear']} files")

if __name__ == "__main__":
    main()