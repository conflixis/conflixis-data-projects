#!/usr/bin/env python3
"""
Clean up invalid files - remove HTML/JS files misnamed as PDFs and other dead files
"""

import os
import json
import subprocess
from pathlib import Path
from datetime import datetime
import shutil

def check_file_type(file_path):
    """Check actual file type using 'file' command"""
    try:
        result = subprocess.run(['file', str(file_path)], 
                              capture_output=True, text=True)
        return result.stdout.strip()
    except:
        return "Unknown"

def identify_invalid_files():
    """Identify all invalid/dead files in the collection"""
    
    base_dir = Path("data/raw/policies_categorized")
    invalid_files_dir = Path("data/raw/invalid_files")
    
    results = {
        'valid_pdfs': [],
        'html_files': [],
        'javascript_files': [],
        'image_files': [],
        'other_invalid': [],
        'tiny_files': [],
        'valid_html': [],
        'valid_doc': []
    }
    
    for file_path in base_dir.rglob('*'):
        if file_path.is_file() and file_path.name != '.gitkeep':
            file_info = {
                'path': str(file_path.relative_to(base_dir)),
                'name': file_path.name,
                'category': file_path.parent.name,
                'size_bytes': file_path.stat().st_size,
                'extension': file_path.suffix.lower()
            }
            
            # Check actual file type
            file_type = check_file_type(file_path)
            file_info['actual_type'] = file_type
            
            # Check for tiny files (likely errors)
            if file_info['size_bytes'] < 5000:  # Less than 5KB
                file_info['reason'] = 'File too small - likely download error'
                results['tiny_files'].append(file_info)
            
            # Check file extension vs actual type
            elif file_info['extension'] == '.pdf':
                if 'PDF document' in file_type:
                    results['valid_pdfs'].append(file_info)
                elif 'HTML' in file_type:
                    file_info['reason'] = 'HTML file misnamed as PDF'
                    results['html_files'].append(file_info)
                elif 'JavaScript' in file_type or 'script' in file_type:
                    file_info['reason'] = 'JavaScript file misnamed as PDF'
                    results['javascript_files'].append(file_info)
                elif 'image' in file_type.lower() or 'PNG' in file_type or 'JPEG' in file_type:
                    file_info['reason'] = 'Image file misnamed as PDF'
                    results['image_files'].append(file_info)
                else:
                    file_info['reason'] = f'Not a PDF: {file_type[:50]}'
                    results['other_invalid'].append(file_info)
                    
            elif file_info['extension'] == '.html':
                if 'HTML' in file_type or 'ASCII text' in file_type:
                    results['valid_html'].append(file_info)
                else:
                    file_info['reason'] = f'Invalid HTML: {file_type[:50]}'
                    results['other_invalid'].append(file_info)
                    
            elif file_info['extension'] == '.doc':
                results['valid_doc'].append(file_info)
            
            else:
                file_info['reason'] = f'Unknown extension: {file_info["extension"]}'
                results['other_invalid'].append(file_info)
    
    return results

def move_invalid_files(results):
    """Move invalid files to separate directory"""
    
    base_dir = Path("data/raw/policies_categorized")
    invalid_dir = Path("data/raw/invalid_files")
    
    # Create directories
    for subdir in ['html_misnamed', 'javascript_misnamed', 'images_misnamed', 
                   'tiny_files', 'other_invalid']:
        (invalid_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    moved_files = []
    
    # Move HTML files misnamed as PDFs
    for file_info in results['html_files']:
        src = base_dir / file_info['path']
        dst = invalid_dir / 'html_misnamed' / file_info['name']
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({'file': file_info['name'], 'type': 'html_misnamed'})
            print(f"Moved HTML: {file_info['name']}")
    
    # Move JavaScript files misnamed as PDFs
    for file_info in results['javascript_files']:
        src = base_dir / file_info['path']
        dst = invalid_dir / 'javascript_misnamed' / file_info['name']
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({'file': file_info['name'], 'type': 'javascript_misnamed'})
            print(f"Moved JS: {file_info['name']}")
    
    # Move image files misnamed as PDFs
    for file_info in results['image_files']:
        src = base_dir / file_info['path']
        dst = invalid_dir / 'images_misnamed' / file_info['name']
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({'file': file_info['name'], 'type': 'image_misnamed'})
            print(f"Moved Image: {file_info['name']}")
    
    # Move tiny files
    for file_info in results['tiny_files']:
        src = base_dir / file_info['path']
        dst = invalid_dir / 'tiny_files' / file_info['name']
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({'file': file_info['name'], 'type': 'tiny_file'})
            print(f"Moved Tiny: {file_info['name']} ({file_info['size_bytes']} bytes)")
    
    # Move other invalid files
    for file_info in results['other_invalid']:
        src = base_dir / file_info['path']
        dst = invalid_dir / 'other_invalid' / file_info['name']
        if src.exists():
            shutil.move(str(src), str(dst))
            moved_files.append({'file': file_info['name'], 'type': 'other_invalid'})
            print(f"Moved Invalid: {file_info['name']}")
    
    return moved_files

def save_cleanup_report(results, moved_files):
    """Save cleanup report"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/invalid_files_cleanup_{timestamp}.json"
    
    summary = {
        'total_files': sum(len(v) for v in results.values()),
        'valid_pdfs': len(results['valid_pdfs']),
        'valid_html': len(results['valid_html']),
        'valid_doc': len(results['valid_doc']),
        'html_misnamed': len(results['html_files']),
        'javascript_misnamed': len(results['javascript_files']),
        'images_misnamed': len(results['image_files']),
        'tiny_files': len(results['tiny_files']),
        'other_invalid': len(results['other_invalid']),
        'total_moved': len(moved_files)
    }
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': summary,
        'detailed_results': results,
        'moved_files': moved_files
    }
    
    os.makedirs("data/output", exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    return report_file, summary

def print_summary(summary):
    """Print cleanup summary"""
    
    print("\n" + "="*60)
    print("INVALID FILES CLEANUP SUMMARY")
    print("="*60)
    
    print(f"\nTotal files examined: {summary['total_files']}")
    print(f"\nVALID FILES:")
    print(f"  Valid PDFs: {summary['valid_pdfs']}")
    print(f"  Valid HTML: {summary['valid_html']}")
    print(f"  Valid DOC: {summary['valid_doc']}")
    
    print(f"\nINVALID/MISNAMED FILES REMOVED:")
    print(f"  HTML files misnamed as PDF: {summary['html_misnamed']}")
    print(f"  JavaScript files misnamed as PDF: {summary['javascript_misnamed']}")
    print(f"  Image files misnamed as PDF: {summary['images_misnamed']}")
    print(f"  Tiny files (< 5KB): {summary['tiny_files']}")
    print(f"  Other invalid: {summary['other_invalid']}")
    print(f"\nTotal files moved: {summary['total_moved']}")

def main():
    """Main execution"""
    
    print("Identifying invalid and misnamed files...")
    results = identify_invalid_files()
    
    print("\nMoving invalid files...")
    moved_files = move_invalid_files(results)
    
    print("\nSaving report...")
    report_file, summary = save_cleanup_report(results, moved_files)
    
    print_summary(summary)
    print(f"\n\nDetailed report saved to: {report_file}")
    
    # Final count
    remaining = summary['valid_pdfs'] + summary['valid_html'] + summary['valid_doc']
    print(f"\n{'='*60}")
    print(f"REMAINING VALID FILES: {remaining}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()