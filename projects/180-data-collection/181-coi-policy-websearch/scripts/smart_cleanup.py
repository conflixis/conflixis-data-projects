#!/usr/bin/env python3
"""
Smart cleanup script that:
1. Keeps HTML files with COI content (renaming them to .html)
2. Removes only truly invalid files (JavaScript, images, tiny/empty files)
3. Preserves all valid PDFs and DOC files
"""

import os
import json
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

def check_file_type(file_path):
    """Check actual file type using 'file' command"""
    try:
        result = subprocess.run(['file', str(file_path)], 
                              capture_output=True, text=True)
        return result.stdout.strip()
    except:
        return "Unknown"

def analyze_html_for_coi(file_path):
    """Analyze HTML content to determine if it's a COI policy"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Try to parse as HTML
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()
        
        if not text or len(text.strip()) < 100:
            return False, 'Too little content'
        
        text_lower = text.lower()
        
        # Check for COI indicators
        coi_indicators = [
            'conflict of interest',
            'conflicts of interest', 
            'coi policy',
            'financial conflict',
            'institutional conflict',
            'financial disclosure',
            'disclosure of interest',
            'management plan',
            'recusal',
            'outside activities',
            'financial interests'
        ]
        
        coi_count = sum(1 for indicator in coi_indicators if indicator in text_lower)
        
        # Check if it's likely a COI policy
        if coi_count >= 3:
            return True, f'Strong COI content ({coi_count} indicators)'
        elif coi_count >= 2:
            return True, f'Likely COI content ({coi_count} indicators)'
        elif 'conflict' in text_lower or 'coi' in text_lower:
            return True, f'Possible COI content'
        else:
            return False, 'No COI indicators found'
            
    except Exception as e:
        return False, f'Error analyzing: {str(e)}'

def smart_cleanup():
    """Perform smart cleanup - keep valid content, remove only truly invalid files"""
    
    base_dir = Path("data/raw/policies_categorized")
    invalid_dir = Path("data/raw/invalid_files")
    
    results = {
        'kept_html_coi': [],
        'renamed_html': [],
        'valid_pdfs': [],
        'valid_doc': [],
        'removed_javascript': [],
        'removed_images': [],
        'removed_tiny': [],
        'removed_non_coi_html': [],
        'errors': []
    }
    
    # Create invalid directories
    for subdir in ['javascript', 'images', 'tiny_files', 'non_coi_html']:
        (invalid_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    for file_path in base_dir.rglob('*'):
        if file_path.is_file() and file_path.name != '.gitkeep':
            file_info = {
                'path': str(file_path.relative_to(base_dir)),
                'name': file_path.name,
                'category': file_path.parent.name,
                'size_bytes': file_path.stat().st_size,
                'extension': file_path.suffix.lower()
            }
            
            file_type = check_file_type(file_path)
            file_info['actual_type'] = file_type
            
            print(f"Checking: {file_info['name'][:50]}...", end=' ')
            
            # Handle tiny/empty files first
            if file_info['size_bytes'] < 1000:  # Less than 1KB
                file_info['reason'] = f'File too small ({file_info["size_bytes"]} bytes)'
                dst = invalid_dir / 'tiny_files' / file_info['name']
                shutil.move(str(file_path), str(dst))
                results['removed_tiny'].append(file_info)
                print("✗ Removed (tiny)")
                continue
            
            # Check based on actual file type
            if 'PDF document' in file_type:
                results['valid_pdfs'].append(file_info)
                print("✓ Valid PDF")
                
            elif 'HTML' in file_type or 'ASCII text' in file_type or 'XML' in file_type:
                # Analyze HTML content for COI
                is_coi, reason = analyze_html_for_coi(file_path)
                file_info['coi_analysis'] = reason
                
                if is_coi:
                    # Keep HTML file but rename if it has .pdf extension
                    if file_info['extension'] == '.pdf':
                        new_name = file_path.stem + '.html'
                        new_path = file_path.parent / new_name
                        shutil.move(str(file_path), str(new_path))
                        file_info['new_name'] = new_name
                        results['renamed_html'].append(file_info)
                        print(f"✓ Renamed to .html (COI content)")
                    else:
                        results['kept_html_coi'].append(file_info)
                        print("✓ Valid HTML (COI content)")
                else:
                    # Move non-COI HTML to invalid folder
                    dst = invalid_dir / 'non_coi_html' / file_info['name']
                    shutil.move(str(file_path), str(dst))
                    results['removed_non_coi_html'].append(file_info)
                    print(f"✗ Removed HTML ({reason})")
                    
            elif 'JavaScript' in file_type or 'script' in file_type:
                # Remove JavaScript files
                dst = invalid_dir / 'javascript' / file_info['name']
                shutil.move(str(file_path), str(dst))
                file_info['reason'] = 'JavaScript file'
                results['removed_javascript'].append(file_info)
                print("✗ Removed (JavaScript)")
                
            elif any(img in file_type for img in ['image', 'PNG', 'JPEG', 'GIF']):
                # Remove image files
                dst = invalid_dir / 'images' / file_info['name']
                shutil.move(str(file_path), str(dst))
                file_info['reason'] = 'Image file'
                results['removed_images'].append(file_info)
                print("✗ Removed (Image)")
                
            elif file_info['extension'] == '.doc':
                results['valid_doc'].append(file_info)
                print("✓ Valid DOC")
                
            else:
                # Unknown type - log but keep for manual review
                file_info['reason'] = f'Unknown type: {file_type[:50]}'
                results['errors'].append(file_info)
                print("? Unknown (kept for review)")
    
    return results

def save_cleanup_report(results):
    """Save detailed cleanup report"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/smart_cleanup_report_{timestamp}.json"
    
    summary = {
        'kept_valid_pdfs': len(results['valid_pdfs']),
        'kept_valid_doc': len(results['valid_doc']),
        'kept_html_with_coi': len(results['kept_html_coi']),
        'renamed_pdf_to_html': len(results['renamed_html']),
        'removed_javascript': len(results['removed_javascript']),
        'removed_images': len(results['removed_images']),
        'removed_tiny_files': len(results['removed_tiny']),
        'removed_non_coi_html': len(results['removed_non_coi_html']),
        'errors_for_review': len(results['errors']),
        'total_kept': len(results['valid_pdfs']) + len(results['valid_doc']) + 
                      len(results['kept_html_coi']) + len(results['renamed_html']),
        'total_removed': len(results['removed_javascript']) + len(results['removed_images']) + 
                        len(results['removed_tiny']) + len(results['removed_non_coi_html'])
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

def print_summary(summary):
    """Print cleanup summary"""
    
    print("\n" + "="*60)
    print("SMART CLEANUP SUMMARY")
    print("="*60)
    
    print(f"\nFILES KEPT ({summary['total_kept']} total):")
    print(f"  Valid PDFs: {summary['kept_valid_pdfs']}")
    print(f"  Valid DOC files: {summary['kept_valid_doc']}")
    print(f"  HTML with COI content: {summary['kept_html_with_coi']}")
    print(f"  PDFs renamed to HTML: {summary['renamed_pdf_to_html']}")
    
    print(f"\nFILES REMOVED ({summary['total_removed']} total):")
    print(f"  JavaScript files: {summary['removed_javascript']}")
    print(f"  Image files: {summary['removed_images']}")
    print(f"  Tiny/empty files: {summary['removed_tiny_files']}")
    print(f"  HTML without COI content: {summary['removed_non_coi_html']}")
    
    if summary['errors_for_review'] > 0:
        print(f"\nFILES FOR REVIEW: {summary['errors_for_review']}")
    
    print("\n" + "="*60)
    print(f"FINAL COLLECTION: {summary['total_kept']} valid COI policy documents")
    print("="*60)

def main():
    """Main execution"""
    
    print("Starting smart cleanup...")
    print("This will preserve HTML files with COI content and remove only invalid files.\n")
    
    results = smart_cleanup()
    
    print("\n\nSaving report...")
    report_file, summary = save_cleanup_report(results)
    
    print_summary(summary)
    print(f"\n\nDetailed report saved to: {report_file}")

if __name__ == "__main__":
    main()