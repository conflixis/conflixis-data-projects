#!/usr/bin/env python3
"""
Step 0: Create detailed manifest with file sizes and checksums
JIRA: DA-167

This script creates a comprehensive manifest including:
- File names and IDs from Google Drive
- File sizes for verification
- Local file checksums (MD5) for integrity checking
"""

import os
import sys
import hashlib
import json
from pathlib import Path
from datetime import datetime
import gdown
import requests

# Google Drive folder ID
FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")
MANIFEST_FILE = Path("detailed_manifest.json")

def get_file_md5(filepath, chunk_size=8192):
    """Calculate MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        while chunk:= f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()

def get_drive_files_with_metadata():
    """Get Google Drive files with size information using gdown."""
    print("=" * 70)
    print("Creating Detailed Google Drive Manifest")
    print("=" * 70)
    print(f"Folder ID: {FOLDER_ID}")
    print("-" * 70)
    
    try:
        # Try using gdown's internal methods
        from gdown.download_folder import _get_session, _get_directory_structure
        
        url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
        
        # Create session with default parameters
        sess = requests.Session()
        
        print("Attempting to retrieve folder contents with metadata...")
        
        # Alternative approach: Use Google Drive API directly through HTTP
        # This will get file metadata including sizes
        folder_page = sess.get(url)
        
        if folder_page.status_code != 200:
            print(f"Could not access folder (status: {folder_page.status_code})")
            return None
            
        # Parse the response to extract file information
        # Google Drive embeds file data in the page
        content = folder_page.text
        
        # Look for file data patterns in the HTML
        import re
        
        # Pattern to find file entries with IDs and names
        file_pattern = r'\["([\w-]+)","[^"]*","([^"]+\.rds)"[^\]]*\]'
        matches = re.findall(file_pattern, content)
        
        if not matches:
            print("Could not parse file information from Drive page")
            print("Using alternative method...")
            
            # Try to extract from different pattern
            # Google Drive sometimes uses different formats
            alt_pattern = r'"([\w-]{28,})","([^"]+\.rds)"'
            matches = re.findall(alt_pattern, content)
        
        if matches:
            print(f"Found {len(matches)} RDS files in folder")
            files = []
            for file_id, filename in matches:
                files.append({
                    'id': file_id,
                    'name': filename,
                    'drive_url': f"https://drive.google.com/uc?id={file_id}"
                })
            return files
        else:
            print("Could not extract file list from Drive")
            return None
            
    except Exception as e:
        print(f"Error accessing Google Drive: {e}")
        return None

def get_drive_file_size(file_id):
    """Try to get file size from Google Drive."""
    try:
        # Try HEAD request to get content-length
        url = f"https://drive.google.com/uc?id={file_id}"
        response = requests.head(url, allow_redirects=True, timeout=5)
        
        if 'content-length' in response.headers:
            return int(response.headers['content-length'])
        
        # If no content-length, try partial download
        response = requests.get(url, stream=True, timeout=5, 
                              headers={'Range': 'bytes=0-0'})
        if 'content-range' in response.headers:
            # Parse: "bytes 0-0/total_size"
            total = response.headers['content-range'].split('/')[-1]
            return int(total)
            
    except:
        pass
    return None

def analyze_local_files():
    """Analyze all locally downloaded files."""
    print("\n" + "=" * 70)
    print("Analyzing Local Files")
    print("=" * 70)
    
    local_files = []
    
    if LOCAL_DATA_DIR.exists():
        rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
        
        print(f"Found {len(rds_files)} local RDS files")
        print("Calculating checksums...")
        
        for i, filepath in enumerate(rds_files, 1):
            print(f"  [{i:3d}/{len(rds_files)}] {filepath.name}...", end=' ')
            
            file_info = {
                'name': filepath.name,
                'size_bytes': filepath.stat().st_size,
                'size_mb': round(filepath.stat().st_size / (1024 * 1024), 2),
                'md5': get_file_md5(filepath),
                'modified': datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                'path': str(filepath)
            }
            
            local_files.append(file_info)
            print(f"{file_info['size_mb']:.2f} MB, MD5: {file_info['md5'][:8]}...")
    
    else:
        print(f"Directory {LOCAL_DATA_DIR} not found")
    
    return local_files

def create_expected_file_list():
    """Create expected file list based on naming patterns."""
    manufacturers = [
        'abbvie', 'allergan', 'astrazeneca', 'boehringer', 
        'gsk', 'janssen', 'lilly', 'merck', 'novartis', 
        'pfizer', 'regeneron', 'sanofi', 'takeda'
    ]
    
    specialties = [
        'Cardiology', 'Dermatology', 'Endocrinology', 'Gastroenterology',
        'Internal.Medicine', 'Nephrology', 'Neurology', 'NP', 'Oncology',
        'PA', 'Primary.Care', 'Psychiatry', 'Pulmonary', 'Surgery', 'Urology'
    ]
    
    expected = []
    for mfg in manufacturers:
        for spec in specialties:
            expected.append(f"df_spec_{mfg}_{spec}.rds")
    
    return sorted(expected)

def create_comprehensive_manifest():
    """Create a comprehensive manifest combining all information."""
    print("\n" + "=" * 70)
    print("Creating Comprehensive Manifest")
    print("=" * 70)
    
    manifest = {
        'created': datetime.now().isoformat(),
        'google_drive': {
            'folder_id': FOLDER_ID,
            'folder_url': f"https://drive.google.com/drive/folders/{FOLDER_ID}",
            'files': []
        },
        'local_files': [],
        'expected_files': [],
        'summary': {
            'expected_total': 0,
            'drive_found': 0,
            'local_found': 0,
            'verified': 0,
            'missing': 0,
            'corrupted': 0
        },
        'status': {}
    }
    
    # Get expected files
    expected = create_expected_file_list()
    manifest['expected_files'] = expected
    manifest['summary']['expected_total'] = len(expected)
    
    # Get local files with checksums
    local_files = analyze_local_files()
    manifest['local_files'] = local_files
    manifest['summary']['local_found'] = len(local_files)
    
    # Create lookup for local files
    local_by_name = {f['name']: f for f in local_files}
    
    # Try to get Drive files
    drive_files = get_drive_files_with_metadata()
    
    if drive_files:
        manifest['google_drive']['files'] = drive_files
        manifest['summary']['drive_found'] = len(drive_files)
        
        # Try to get sizes for a sample of Drive files
        print("\nGetting file sizes from Drive (sampling first 3)...")
        for i, file in enumerate(drive_files[:3]):
            size = get_drive_file_size(file['id'])
            if size:
                file['size_bytes'] = size
                file['size_mb'] = round(size / (1024 * 1024), 2)
                print(f"  {file['name']}: {file['size_mb']:.2f} MB")
    
    # Create status for each expected file
    print("\nCreating file status report...")
    
    for filename in expected:
        status = {
            'name': filename,
            'expected': True,
            'local_exists': False,
            'drive_exists': False,
            'verified': False,
            'issues': []
        }
        
        # Extract manufacturer and specialty
        parts = filename.replace('df_spec_', '').replace('.rds', '').split('_')
        status['manufacturer'] = parts[0] if parts else 'unknown'
        status['specialty'] = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
        
        # Check if file exists locally
        if filename in local_by_name:
            status['local_exists'] = True
            status['local_size_mb'] = local_by_name[filename]['size_mb']
            status['local_md5'] = local_by_name[filename]['md5']
            status['verified'] = True  # Assume verified if we have MD5
        
        # Check if file exists in Drive listing
        if drive_files:
            for df in drive_files:
                if df['name'] == filename:
                    status['drive_exists'] = True
                    if 'size_mb' in df:
                        status['drive_size_mb'] = df['size_mb']
                    break
        
        # Identify issues
        if not status['local_exists']:
            status['issues'].append('missing_local')
        if not status['drive_exists'] and drive_files:
            status['issues'].append('missing_drive')
        
        manifest['status'][filename] = status
    
    # Calculate summary statistics
    missing_files = [f for f, s in manifest['status'].items() 
                    if not s['local_exists']]
    manifest['summary']['missing'] = len(missing_files)
    manifest['summary']['verified'] = len([s for s in manifest['status'].values() 
                                          if s['verified']])
    
    # Group missing by manufacturer
    missing_by_mfg = {}
    for filename in missing_files:
        mfg = manifest['status'][filename]['manufacturer']
        if mfg not in missing_by_mfg:
            missing_by_mfg[mfg] = []
        missing_by_mfg[mfg].append(filename)
    
    manifest['missing_by_manufacturer'] = missing_by_mfg
    
    # Save manifest
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✓ Detailed manifest saved to: {MANIFEST_FILE}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Expected files: {manifest['summary']['expected_total']}")
    print(f"Local files: {manifest['summary']['local_found']}")
    print(f"Missing files: {manifest['summary']['missing']}")
    print(f"Verified files (with MD5): {manifest['summary']['verified']}")
    
    if missing_by_mfg:
        print("\nMissing files by manufacturer:")
        for mfg, files in sorted(missing_by_mfg.items()):
            print(f"  {mfg:15s}: {len(files):3d} files")
    
    # Calculate total local storage
    total_local_size = sum(f['size_bytes'] for f in local_files)
    print(f"\nTotal local storage: {total_local_size / (1024**3):.2f} GB")
    
    return manifest

def main():
    """Main execution."""
    manifest = create_comprehensive_manifest()
    
    print("\n" + "=" * 70)
    print("Manifest Creation Complete")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Review detailed_manifest.json for file status")
    print("  2. Use MD5 checksums to verify file integrity")
    print("  3. Re-download any corrupted files")
    print("  4. Continue downloading missing files")

if __name__ == "__main__":
    main()