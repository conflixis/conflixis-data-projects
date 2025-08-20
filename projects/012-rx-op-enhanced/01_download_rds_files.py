#!/usr/bin/env python3
"""
Download RDS files using service account credentials
JIRA: DA-167

This script uses the service account from environment variables.
"""

import os
import io
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Load environment variables
load_dotenv('../../.env')

FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")

def authenticate_with_service_account():
    """Authenticate using service account from environment."""
    try:
        # Get service account JSON from environment
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        
        if not service_account_json:
            print("✗ No service account key found in environment")
            return None
        
        # Parse the JSON (it's escaped in the env var)
        service_account_info = json.loads(service_account_json)
        
        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        
        print("✓ Authenticated with service account: " + service_account_info['client_email'])
        
        # Build the Drive service
        return build('drive', 'v3', credentials=credentials)
        
    except Exception as e:
        print(f"✗ Error authenticating: {e}")
        return None

def list_files_in_folder(service, folder_id):
    """List all files in a Google Drive folder."""
    try:
        results = []
        page_token = None
        
        print(f"Listing files in folder {folder_id}...")
        
        while True:
            try:
                response = service.files().list(
                    q=f"'{folder_id}' in parents",
                    spaces='drive',
                    fields='nextPageToken, files(id, name, size, mimeType)',
                    pageToken=page_token,
                    pageSize=1000,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
                
                files = response.get('files', [])
                results.extend(files)
                print(f"  Found {len(files)} files in this page...")
                
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break
                    
            except HttpError as error:
                if 'insufficientPermissions' in str(error) or '403' in str(error):
                    print(f"\n✗ Permission denied. The folder may not be shared with the service account.")
                    print(f"   Service account: {service._http.credentials.service_account_email}")
                    print(f"\nTo fix this:")
                    print(f"1. Open the folder in Google Drive: https://drive.google.com/drive/folders/{folder_id}")
                    print(f"2. Click 'Share' button")
                    print(f"3. Add this email: {service._http.credentials.service_account_email}")
                    print(f"4. Give 'Viewer' or 'Editor' permission")
                    return []
                else:
                    print(f"Error listing files: {error}")
                    return []
        
        return results
        
    except Exception as e:
        print(f'An error occurred: {e}')
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
        downloader = MediaIoBaseDownload(fh, request, chunksize=50*1024*1024)  # 50MB chunks for faster download
        
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
        if '403' in str(error):
            return False, "Permission denied"
        return False, f"Error: {str(error)[:100]}"

def main():
    """Main execution."""
    print("=" * 70)
    print("RX-OP Enhanced: Download with Service Account")
    print("=" * 70)
    
    # Create directory if needed
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    
    # Check existing files
    existing = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"\nExisting files: {len(existing)}")
    
    # Authenticate
    print("\nAuthenticating...")
    service = authenticate_with_service_account()
    
    if not service:
        print("\n✗ Authentication failed")
        return
    
    # List files in folder
    print(f"\nAccessing folder...")
    files = list_files_in_folder(service, FOLDER_ID)
    
    if not files:
        print("\n✗ Could not access folder")
        print("\nThe folder needs to be shared with the service account.")
        return
    
    # Filter for RDS files
    rds_files = [f for f in files if f['name'].endswith('.rds')]
    
    print(f"\n✓ Found {len(rds_files)} RDS files in Drive")
    print("-" * 70)
    
    # Download each file
    successful = 0
    failed = []
    skipped = 0
    
    for i, file_info in enumerate(rds_files, 1):
        file_name = file_info['name']
        file_id = file_info['id']
        output_path = LOCAL_DATA_DIR / file_name
        
        # Quick skip check without printing
        if output_path.exists():
            skipped += 1
            successful += 1
            continue  # Skip to next file immediately
        
        # Only print for files we're actually downloading
        print(f"[{i:3d}/{len(rds_files)}] {file_name[:50]:50s} ... ", end='')
        
        success, message = download_file(service, file_id, file_name, LOCAL_DATA_DIR)
        print(f"{'✓' if success else '✗'} {message}")
        
        if success:
            successful += 1
        else:
            failed.append(file_name)
        
        # No delay - download as fast as possible
    
    # Summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    print(f"Total files in Drive: {len(rds_files)}")
    print(f"Already downloaded (skipped): {skipped}")
    print(f"Newly downloaded: {successful - skipped}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for f in failed[:5]:
            print(f"  - {f}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    
    # Final count
    final_count = len(list(LOCAL_DATA_DIR.glob("*.rds")))
    print(f"\nTotal RDS files in directory: {final_count}/195")
    
    if final_count >= 195:
        print("\n✅ SUCCESS! All 195 files downloaded!")
    else:
        print(f"\n⚠️  Missing {195 - final_count} files")

if __name__ == "__main__":
    main()