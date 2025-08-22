#!/usr/bin/env python3
"""
Download RDS files using authenticated Google Drive API
JIRA: DA-167

This script uses OAuth2 authentication to bypass rate limits.
"""

import os
import io
import time
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")
TOKEN_FILE = Path("token.json")
CREDS_FILE = Path("credentials.json")

def authenticate():
    """Authenticate and return Google Drive service."""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDS_FILE.exists():
                print("\n" + "=" * 70)
                print("AUTHENTICATION REQUIRED")
                print("=" * 70)
                print("\nTo download files with authentication, you need to:")
                print("\n1. Go to: https://console.cloud.google.com/apis/credentials")
                print("2. Create OAuth 2.0 Client ID (Desktop application)")
                print("3. Download the credentials JSON")
                print(f"4. Save it as: {CREDS_FILE}")
                print("\nAlternatively, use gcloud CLI:")
                print("  gcloud auth application-default login")
                print("=" * 70)
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    return build('drive', 'v3', credentials=creds)

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
        return []

def download_file(service, file_id, file_name, output_dir):
    """Download a file from Google Drive."""
    output_path = output_dir / file_name
    
    # Skip if already exists
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ Already exists: {file_name} ({size_mb:.2f} MB)")
        return True
    
    try:
        request = service.files().get_media(fileId=file_id)
        
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        
        done = False
        print(f"  Downloading {file_name}...", end=' ')
        
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"{int(status.progress() * 100)}%", end=' ')
        
        # Write to file
        fh.seek(0)
        with open(output_path, 'wb') as f:
            f.write(fh.read())
        
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ ({size_mb:.2f} MB)")
        return True
        
    except HttpError as error:
        print(f"\n  ✗ Error downloading {file_name}: {error}")
        return False

def main():
    """Main execution."""
    print("=" * 70)
    print("RX-OP Enhanced: Authenticated Google Drive Download")
    print("=" * 70)
    
    # Create directory if needed
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    
    # Authenticate
    print("\nAuthenticating...")
    service = authenticate()
    
    if not service:
        print("\n✗ Authentication failed. Please set up credentials.")
        return
    
    print("✓ Authentication successful")
    
    # List files in folder
    print(f"\nListing files in folder: {FOLDER_ID}")
    files = list_files_in_folder(service, FOLDER_ID)
    
    if not files:
        print("✗ No files found or error accessing folder")
        return
    
    # Filter for RDS files
    rds_files = [f for f in files if f['name'].endswith('.rds')]
    
    print(f"Found {len(rds_files)} RDS files")
    
    # Check existing files
    existing = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"Already downloaded: {len(existing)} files")
    print("-" * 70)
    
    # Download each file
    successful = 0
    failed = []
    
    for i, file_info in enumerate(rds_files, 1):
        file_name = file_info['name']
        file_id = file_info['id']
        file_size = file_info.get('size', 'Unknown')
        
        print(f"\n[{i}/{len(rds_files)}] {file_name}")
        if file_size != 'Unknown':
            print(f"  Size: {int(file_size) / (1024*1024):.2f} MB")
        
        if download_file(service, file_id, file_name, LOCAL_DATA_DIR):
            successful += 1
        else:
            failed.append(file_name)
        
        # Small delay between downloads
        if i < len(rds_files):
            time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Total files: {len(rds_files)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {f}")
    
    # Final count
    final_count = len(list(LOCAL_DATA_DIR.glob("*.rds")))
    print(f"\nTotal RDS files in directory: {final_count}")
    
    if final_count >= 195:
        print("\n✓ All expected files downloaded!")
    else:
        print(f"\n⚠ Still missing {195 - final_count} files")

if __name__ == "__main__":
    main()