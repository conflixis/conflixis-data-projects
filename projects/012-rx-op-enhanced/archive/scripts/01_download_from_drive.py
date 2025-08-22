#!/usr/bin/env python3
"""
Step 1: Download RDS files from Google Drive
JIRA: DA-167

This script downloads all RDS files from the specified Google Drive folder.
"""

import os
import sys
import gdown
from pathlib import Path

# Google Drive folder ID from the shared link
# https://drive.google.com/drive/folders/1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT
FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")

def download_from_drive_folder():
    """Download all files from Google Drive folder."""
    print("=" * 70)
    print("RX-OP Enhanced: Download from Google Drive")
    print("=" * 70)
    print(f"Folder ID: {FOLDER_ID}")
    print(f"Target directory: {LOCAL_DATA_DIR}")
    print("-" * 70)
    
    # Create directory if it doesn't exist
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    
    # Check existing files
    existing_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"\nExisting files: {len(existing_files)}")
    
    # Use gdown to download the entire folder
    url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
    
    try:
        print("\nStarting download...")
        print("Note: This may take a while for large files.")
        print("Will continue even if some files fail (rate limits).\n")
        
        # Download folder to local directory
        # quiet=False shows progress, use_cookies=False for public folders
        gdown.download_folder(
            url, 
            output=str(LOCAL_DATA_DIR), 
            quiet=False, 
            use_cookies=False,
            remaining_ok=True,  # Continue even if some files fail
            resume=True  # Resume partial downloads
        )
        
        print(f"\n✓ Files downloaded to {LOCAL_DATA_DIR}")
        
        # List downloaded files
        print("\nDownloaded files:")
        print("-" * 70)
        rds_files = list(LOCAL_DATA_DIR.glob("*.rds"))
        
        if rds_files:
            for i, file in enumerate(rds_files, 1):
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"  {i:2d}. {file.name:50s} {size_mb:10.2f} MB")
            
            total_size_mb = sum(f.stat().st_size for f in rds_files) / (1024 * 1024)
            print("-" * 70)
            print(f"  Total: {len(rds_files)} files, {total_size_mb:.2f} MB")
        else:
            print("  No RDS files found in download.")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error downloading from Google Drive: {e}")
        print("\nPossible issues:")
        print("  1. The folder may not be publicly accessible")
        print("  2. Google Drive download limits may have been reached")
        print("  3. Network connectivity issues")
        print("\nYou may need to:")
        print("  - Check the sharing settings on the Google Drive folder")
        print("  - Download files manually and place in:", LOCAL_DATA_DIR)
        print("  - Try again later if rate limited")
        return False

def main():
    """Main execution."""
    success = download_from_drive_folder()
    
    if success:
        print("\n" + "=" * 70)
        print("✓ Download complete!")
        print("  Next step: Run 02_analyze_rds_files.py")
        print("=" * 70)
        sys.exit(0)
    else:
        print("\n" + "=" * 70)
        print("✗ Download failed!")
        print("  Please check the error messages above.")
        print("=" * 70)
        sys.exit(1)

if __name__ == "__main__":
    main()