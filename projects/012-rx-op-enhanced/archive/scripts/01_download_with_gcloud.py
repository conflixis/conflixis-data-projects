#!/usr/bin/env python3
"""
Download RDS files using gcloud authentication
JIRA: DA-167

This script uses gcloud default credentials to access Google Drive.
"""

import os
import io
import time
import json
from pathlib import Path
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")

def authenticate_with_gcloud():
    """Authenticate using gcloud default credentials."""
    try:
        # Try to use Application Default Credentials
        creds, project = default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
        print(f"✓ Using default credentials (Project: {project})")
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"✗ Could not use default credentials: {e}")
        print("\nPlease run: gcloud auth application-default login")
        print("Then try again.")
        return None

def list_files_in_folder(service, folder_id):
    """List all files in a Google Drive folder."""
    try:
        results = []
        page_token = None
        
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents",
                spaces='drive',
                fields='nextPageToken, files(id, name, size, mimeType)',
                pageToken=page_token,
                pageSize=1000
            ).execute()
            
            results.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            
            if page_token is None:
                break
        
        return results
    
    except HttpError as error:
        print(f'An error occurred: {error}')
        # If it's a permission error, the folder might need different access
        if 'insufficientPermissions' in str(error):
            print("\nThe folder may not be accessible with current credentials.")
            print("Make sure the folder is shared with your Google account.")
        return []

def download_file(service, file_id, file_name, output_dir):
    """Download a file from Google Drive."""
    output_path = output_dir / file_name
    
    # Skip if already exists
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        return True, f"Already exists ({size_mb:.2f} MB)"
    
    try:
        request = service.files().get_media(fileId=file_id)
        
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)  # 10MB chunks
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        # Write to file
        fh.seek(0)
        with open(output_path, 'wb') as f:
            f.write(fh.read())
        
        size_mb = output_path.stat().st_size / (1024 * 1024)
        return True, f"Downloaded ({size_mb:.2f} MB)"
        
    except HttpError as error:
        return False, f"Error: {str(error)[:100]}"

def main():
    """Main execution."""
    print("=" * 70)
    print("RX-OP Enhanced: Google Drive Download with gcloud")
    print("=" * 70)
    
    # Create directory if needed
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    
    # Check existing files first
    existing = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"\nExisting files: {len(existing)}")
    
    # Authenticate
    print("\nAuthenticating with gcloud...")
    service = authenticate_with_gcloud()
    
    if not service:
        return
    
    # List files in folder
    print(f"\nAccessing folder: {FOLDER_ID}")
    files = list_files_in_folder(service, FOLDER_ID)
    
    if not files:
        print("✗ Could not access folder or no files found")
        print("\nTrying alternative: Download with gdown using cookies...")
        
        # Try gdown with browser cookies
        print("\nYou can try:")
        print("1. Open the folder in your browser while logged in")
        print("2. Use browser extension to export cookies")
        print("3. Use gdown with cookies file")
        return
    
    # Filter for RDS files
    rds_files = [f for f in files if f['name'].endswith('.rds')]
    
    print(f"Found {len(rds_files)} RDS files in Drive")
    print("-" * 70)
    
    # Download each file
    successful = 0
    failed = []
    
    for i, file_info in enumerate(rds_files, 1):
        file_name = file_info['name']
        file_id = file_info['id']
        
        print(f"[{i}/{len(rds_files)}] {file_name}... ", end='')
        
        success, message = download_file(service, file_id, file_name, LOCAL_DATA_DIR)
        print(f"{'✓' if success else '✗'} {message}")
        
        if success:
            successful += 1
        else:
            failed.append(file_name)
        
        # Small delay between downloads
        if i < len(rds_files):
            time.sleep(0.2)
    
    # Summary
    print("\n" + "=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Total files: {len(rds_files)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for f in failed[:10]:  # Show first 10
            print(f"  - {f}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
    
    # Final count
    final_count = len(list(LOCAL_DATA_DIR.glob("*.rds")))
    print(f"\nTotal RDS files downloaded: {final_count}/195")
    
    if final_count >= 195:
        print("\n✓ All files downloaded successfully!")
    else:
        print(f"\n⚠ Missing {195 - final_count} files")

if __name__ == "__main__":
    main()