#!/usr/bin/env python3
"""
Step 0: Create manifest of all files in Google Drive folder
JIRA: DA-167

This script lists all files in the Google Drive folder to create a complete manifest.
"""

import os
import sys
import gdown
from pathlib import Path
import json
import requests

# Google Drive folder ID from the shared link
FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
MANIFEST_FILE = Path("google_drive_manifest.json")
LOCAL_DATA_DIR = Path("mfg-spec-data")

def get_drive_folder_contents():
    """Get list of all files in Google Drive folder."""
    print("=" * 70)
    print("Creating Google Drive Manifest")
    print("=" * 70)
    print(f"Folder ID: {FOLDER_ID}")
    print("-" * 70)
    
    # Use gdown's internal API to get folder contents
    import gdown.download_folder
    
    try:
        # Get folder info
        url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
        
        # Use gdown's parse_folder_url to get file list
        from gdown.download_folder import _parse_google_drive_file
        
        # Alternative: Use requests to get folder page and parse
        print("\nFetching folder contents from Google Drive...")
        
        # Try using gdown's folder listing capability
        import gdown.parse_url
        
        # Get the folder listing
        folder_url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
        
        # Use gdown's internal method to get file list
        from gdown.download_folder import _get_session, _get_directory_structure
        
        sess = _get_session()
        
        print("Retrieving folder structure...")
        file_list, _ = _get_directory_structure(url, sess)
        
        if not file_list:
            print("Could not retrieve file list. Trying alternative method...")
            # Try to get at least the files we know about
            return None
        
        # Create manifest
        manifest = {
            'folder_id': FOLDER_ID,
            'folder_url': url,
            'total_files': len(file_list),
            'files': []
        }
        
        print(f"\nFound {len(file_list)} files in Google Drive folder")
        print("-" * 70)
        
        # Process each file
        for file_id, file_info in file_list.items():
            file_entry = {
                'id': file_id,
                'name': file_info['name'],
                'url': f"https://drive.google.com/uc?id={file_id}"
            }
            manifest['files'].append(file_entry)
            print(f"  {file_info['name']}")
        
        # Sort files by name for consistency
        manifest['files'].sort(key=lambda x: x['name'])
        
        # Save manifest
        with open(MANIFEST_FILE, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print("\n" + "=" * 70)
        print(f"✓ Manifest created: {MANIFEST_FILE}")
        print(f"  Total files in Drive: {len(file_list)}")
        
        return manifest
        
    except Exception as e:
        print(f"\n✗ Error creating manifest: {e}")
        print("\nFalling back to manual manifest creation...")
        return None

def create_manual_manifest():
    """Create manifest from known patterns."""
    print("\nCreating manifest from known patterns...")
    
    # Known manufacturers and specialties
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
    
    manifest = {
        'folder_id': FOLDER_ID,
        'folder_url': f"https://drive.google.com/drive/folders/{FOLDER_ID}",
        'files': [],
        'expected_files': []
    }
    
    # Generate expected file list
    for mfg in manufacturers:
        for spec in specialties:
            filename = f"df_spec_{mfg}_{spec}.rds"
            manifest['expected_files'].append(filename)
    
    manifest['expected_total'] = len(manifest['expected_files'])
    
    print(f"Expected files based on pattern: {manifest['expected_total']}")
    print(f"  Manufacturers: {len(manufacturers)}")
    print(f"  Specialties: {len(specialties)}")
    print(f"  Total combinations: {len(manufacturers)} x {len(specialties)} = {manifest['expected_total']}")
    
    # Save manifest
    with open('expected_manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✓ Expected manifest saved to: expected_manifest.json")
    
    return manifest

def check_downloaded_files(manifest):
    """Check which files have been downloaded."""
    print("\n" + "=" * 70)
    print("Checking Downloaded Files")
    print("=" * 70)
    
    # Get list of downloaded files
    downloaded_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    downloaded_names = set(f.name for f in downloaded_files)
    
    print(f"Downloaded files: {len(downloaded_files)}")
    
    if manifest and 'files' in manifest and manifest['files']:
        # Check against actual manifest
        total_expected = len(manifest['files'])
        manifest_names = set(f['name'] for f in manifest['files'])
        
        missing = manifest_names - downloaded_names
        extra = downloaded_names - manifest_names
        
        print(f"Expected files (from Drive): {total_expected}")
        print(f"Missing files: {len(missing)}")
        print(f"Extra files (not in manifest): {len(extra)}")
        
        if missing:
            print("\nMissing files:")
            for name in sorted(missing):
                print(f"  - {name}")
        
        if extra:
            print("\nExtra files:")
            for name in sorted(extra):
                print(f"  + {name}")
    
    elif manifest and 'expected_files' in manifest:
        # Check against expected pattern
        expected_names = set(manifest['expected_files'])
        
        missing = expected_names - downloaded_names
        found = expected_names & downloaded_names
        
        print(f"Expected files (by pattern): {len(expected_names)}")
        print(f"Found: {len(found)}")
        print(f"Missing: {len(missing)}")
        
        # Group missing by manufacturer
        missing_by_mfg = {}
        for name in missing:
            parts = name.replace('df_spec_', '').replace('.rds', '').split('_')
            if parts:
                mfg = parts[0]
                if mfg not in missing_by_mfg:
                    missing_by_mfg[mfg] = []
                missing_by_mfg[mfg].append(name)
        
        if missing_by_mfg:
            print("\nMissing files by manufacturer:")
            for mfg, files in sorted(missing_by_mfg.items()):
                print(f"  {mfg}: {len(files)} files")
    
    # Save download status
    status = {
        'downloaded': sorted(list(downloaded_names)),
        'downloaded_count': len(downloaded_names),
        'timestamp': str(Path.cwd())
    }
    
    with open('download_status.json', 'w') as f:
        json.dump(status, f, indent=2)
    
    print(f"\n✓ Download status saved to: download_status.json")

def main():
    """Main execution."""
    # Try to get actual manifest from Drive
    manifest = get_drive_folder_contents()
    
    if not manifest:
        # Fall back to pattern-based manifest
        manifest = create_manual_manifest()
    
    # Check what we've downloaded
    check_downloaded_files(manifest)
    
    print("\n" + "=" * 70)
    print("Manifest Creation Complete")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Review the manifest files")
    print("  2. Run 01_download_from_drive.py to download missing files")
    print("  3. Run 02_analyze_rds_files.py to analyze structure")

if __name__ == "__main__":
    main()