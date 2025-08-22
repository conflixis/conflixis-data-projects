#!/usr/bin/env python3
"""
Step 1B: Download RDS files with retry logic and rate limit handling
JIRA: DA-167

This script downloads files individually with retry logic and delays.
"""

import os
import sys
import time
import json
import gdown
from pathlib import Path
import hashlib
import requests

# Configuration
FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")
MANIFEST_FILE = Path("detailed_manifest.json")
MAX_RETRIES = 3
DELAY_BETWEEN_FILES = 2  # seconds
DELAY_AFTER_ERROR = 10  # seconds

# Known file IDs from the Google Drive listing
FILE_IDS = {
    # Allergan remaining files
    "df_spec_allergan_Internal.Medicine.rds": "18zvB6RJ0OMGnFH2G39FYf-LK330IoniM",
    "df_spec_allergan_Nephrology.rds": "1TMk5qa9RQzHeDFbL66wI9O9fmvOsAMzs",
    "df_spec_allergan_Neurology.rds": "1OKaBryJPaGGhOA-6ivl_5PLleGkpFxYW",
    "df_spec_allergan_NP.rds": "1JQq4EkfzMCdfPxhyrXKrJSGUVL1A0F6p",
    "df_spec_allergan_Oncology.rds": "16D17lpzv-zzxDh8spIXmklKHhU6fq-Lx",
    "df_spec_allergan_PA.rds": "1vD5-4kQ4Yp_g9QSeaB32Afg8DdmwYG5T",
    "df_spec_allergan_Primary.Care.rds": "1FVxdA9GmFKm_JDuIWxHoutVuEjmpVUDn",
    "df_spec_allergan_Psychiatry.rds": "1D1HYRDEbMC7UnvSAIkSDPq2h7UuCKa3c",
    "df_spec_allergan_Pulmonary.rds": "18xATrbKIgurONEZS6dkLeefmd_t00qEq",
    "df_spec_allergan_Surgery.rds": "1OoIw3D91rFxTnQb57b2gRl5hJeMrwAo6",
    "df_spec_allergan_Urology.rds": "1YGWlayosZnsooVNaTDKXqwk6-iEkv_vQ",
    
    # AstraZeneca files
    "df_spec_astrazeneca_Cardiology.rds": "1U8qVruPutRkeRANxC-dRtlUiMoZUVeS-",
    "df_spec_astrazeneca_Dermatology.rds": "1QwXEY3XkfVPPy-1GJr9u3yOsgTQbKqgZ",
    "df_spec_astrazeneca_Endocrinology.rds": "1Gb1vbT0BKZ_loav2X2ZsVz974JekNqE8",
    "df_spec_astrazeneca_Gastroenterology.rds": "18szrL2vGypkge88wTb7dz9UbVMXTFXqg",
    "df_spec_astrazeneca_Internal.Medicine.rds": "1mrcsODNB_AU3HkLQYh9i87RuXrT2Zxe4",
    "df_spec_astrazeneca_Nephrology.rds": "1w59b1Bj_h5V7tGQ-44XlU8JoG-bGlmo3",
    "df_spec_astrazeneca_Neurology.rds": "1YTUh4nIQfCbE0PxOpWcZCWYsAhR_MC9G",
    "df_spec_astrazeneca_NP.rds": "1pUvz02GX_LbMRH5AysG2kGhPZ7-NHzs6",
    "df_spec_astrazeneca_Oncology.rds": "1Ugt1yqboOnSFhNIKNMUGhyuIpOUjmhVM",
    "df_spec_astrazeneca_PA.rds": "1Zx7ockRYPnsVHqrc_ToCEGAvytV2XvOJ",
    "df_spec_astrazeneca_Primary.Care.rds": "1e9LOFgScFELqh3d33DzFvAya125UATff",
    "df_spec_astrazeneca_Psychiatry.rds": "1RC2-YIaTxJILZn6oD_jx1vwn6NA88J0V",
    "df_spec_astrazeneca_Pulmonary.rds": "17G5jExOd8NrKBNNpvqSUTK6IMu4dv0Lf",
    "df_spec_astrazeneca_Surgery.rds": "1DGSYzmaNrfzHu0WONpOqBpe0KnJi485_",
    "df_spec_astrazeneca_Urology.rds": "10wLUUjSqIvUYn0lRoel1PiuCibvap8VR",
    
    # Boehringer files
    "df_spec_boehringer_Cardiology.rds": "1aGrhvO3DOaggHlHcpezGMBkpZAnIBxGm",
    "df_spec_boehringer_Dermatology.rds": "1oblEwPNPwe0YgUNtYn-UdvxoFOgbgWzf",
    "df_spec_boehringer_Endocrinology.rds": "1S8Kp-Fyk1huuiJWC-jpdOupobD7ro9kT",
    "df_spec_boehringer_Gastroenterology.rds": "1AmTjRGMuiUb12EiisTCjDcysWOapmKu3",
    "df_spec_boehringer_Internal.Medicine.rds": "1R0yZIe6fgCuNM4mqD9ytQ8cE10M9HE6O",
}

def download_single_file(filename, file_id, output_dir):
    """Download a single file from Google Drive with retry logic."""
    output_path = output_dir / filename
    
    # Skip if already exists
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ Already exists: {filename} ({size_mb:.2f} MB)")
        return True
    
    url = f"https://drive.google.com/uc?id={file_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  [{attempt+1}/{MAX_RETRIES}] Downloading {filename}...")
            
            # Use gdown to download
            output_file = gdown.download(url, str(output_path), quiet=False)
            
            if output_file and output_path.exists():
                size_mb = output_path.stat().st_size / (1024 * 1024)
                print(f"  ✓ Downloaded: {filename} ({size_mb:.2f} MB)")
                return True
            else:
                print(f"  ✗ Download failed: {filename}")
                
        except Exception as e:
            print(f"  ✗ Error on attempt {attempt+1}: {str(e)[:100]}")
            
        if attempt < MAX_RETRIES - 1:
            print(f"  Waiting {DELAY_AFTER_ERROR} seconds before retry...")
            time.sleep(DELAY_AFTER_ERROR)
    
    print(f"  ✗ Failed after {MAX_RETRIES} attempts: {filename}")
    return False

def get_file_ids_from_manifest():
    """Extract file IDs from gdown's internal tracking if available."""
    # Check if gdown has cached the file list
    gdrive_dir = LOCAL_DATA_DIR / "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
    if gdrive_dir.exists():
        print(f"Found Google Drive cache directory: {gdrive_dir}")
        # Move files from cache to main directory
        for file in gdrive_dir.glob("*.rds"):
            target = LOCAL_DATA_DIR / file.name
            if not target.exists():
                file.rename(target)
                print(f"  Moved {file.name} from cache")
    
    return FILE_IDS

def main():
    """Main execution."""
    print("=" * 70)
    print("RX-OP Enhanced: Download with Retry Logic")
    print("=" * 70)
    
    # Create directory if needed
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    
    # Check existing files
    existing = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"Existing files: {len(existing)}")
    
    # Get file IDs
    file_ids = get_file_ids_from_manifest()
    
    print(f"Files to download: {len(file_ids)}")
    print(f"Delay between files: {DELAY_BETWEEN_FILES} seconds")
    print(f"Max retries per file: {MAX_RETRIES}")
    print("-" * 70)
    
    # Download each file
    successful = 0
    failed = []
    
    for i, (filename, file_id) in enumerate(file_ids.items(), 1):
        print(f"\n[{i}/{len(file_ids)}] Processing {filename}")
        
        if download_single_file(filename, file_id, LOCAL_DATA_DIR):
            successful += 1
        else:
            failed.append(filename)
        
        # Delay between downloads to avoid rate limits
        if i < len(file_ids):
            time.sleep(DELAY_BETWEEN_FILES)
    
    # Summary
    print("\n" + "=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Successful: {successful}/{len(file_ids)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {f}")
        
        # Save failed list for retry
        with open("failed_downloads.txt", "w") as f:
            for filename in failed:
                f.write(f"{filename}\n")
        print("\nFailed files saved to: failed_downloads.txt")
    
    # Final count
    final_count = len(list(LOCAL_DATA_DIR.glob("*.rds")))
    print(f"\nTotal RDS files in directory: {final_count}")
    
    if final_count >= 195:
        print("\n✓ All expected files downloaded!")
    else:
        print(f"\n⚠ Still missing {195 - final_count} files")
        print("  You may need to:")
        print("  1. Wait for rate limits to reset (usually 24 hours)")
        print("  2. Download remaining files manually")
        print("  3. Ask the file owner to create a ZIP archive")

if __name__ == "__main__":
    main()