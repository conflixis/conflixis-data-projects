#!/usr/bin/env python3
"""
Download COI policies from page 2 of Google search results
"""

import os
import json
import requests
import hashlib
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URLs from Google search page 2
page2_policies = [
    {"org": "University of South Carolina", "url": "https://www.sc.edu/study/colleges_schools/medicine/internal/documents/som_conflict_of_interest_policy.pdf"},
    {"org": "Hospital for Special Surgery (HSS)", "url": "https://www.hss.edu/contentassets/4abd50ba30fa4c5687615baee72cc172/hss-conflict-of-interest-policy-appendix.pdf"},
    {"org": "Prisma Health", "url": "https://prismahealth.org/getmedia/76a9ff04-3a2a-47d3-9aa0-e503883bca2e/Prisma-Health-Conflict-of-Interest.pdf"},
    {"org": "Brown University Health", "url": "https://www.brownhealth.org/sites/default/files/2021-11/CCPM-43-003.pdf"},
    {"org": "Stony Brook Medicine", "url": "https://www.stonybrookmedicine.edu/sites/default/files/SB-ACO_ConflictofInterestPolicy.2024_1.pdf"},
    {"org": "Albany Med Health System", "url": "https://www.albanymed.org/wp-content/uploads/sites/2/2023/03/Conflict-of-Interest-Policy-System-Rev-12-14-22-031023.pdf"},
    {"org": "Mayo Clinic Research", "url": "https://www.mayoclinic.org/documents/conflict-of-interest-research-policy-addendum/doc-20562490"},
    {"org": "Nemours Children's Health", "url": "https://www.nemours.org/content/dam/reading-brightstart/documents/coipolicy.pdf"},
    {"org": "Virtua Health", "url": "https://www.virtua.org/-/media/Project/Virtua-Tenant/Virtua/PDFs/Vendor-Information/Public-Health-Service-PHS-Research-Financial-Conflict-of-Interest-Policy0822.pdf"},
    {"org": "American Society of Gene & Cell Therapy", "url": "https://www.asgct.org/global/documents/fcoi-policy-asgct.aspx"},
]

def download_policy(org, url, download_dir="data/raw/policies"):
    """Download a single policy PDF"""
    os.makedirs(download_dir, exist_ok=True)
    
    try:
        # Create filename
        org_clean = org.replace(' ', '_').replace('/', '_').replace('&', 'and')
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        
        # Determine extension
        if '.pdf' in url.lower():
            ext = 'pdf'
        elif '.aspx' in url.lower() or '.html' in url.lower():
            ext = 'html'
        else:
            ext = 'pdf'  # Default to PDF
            
        filename = f"{download_dir}/{org_clean}_{url_hash}.{ext}"
        
        # Check if already downloaded
        if os.path.exists(filename):
            logger.info(f"Already downloaded: {filename}")
            return {'success': True, 'filename': filename, 'status': 'already_exists'}
        
        # Download file
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"✓ Downloaded: {org} -> {filename}")
            return {
                'success': True,
                'filename': filename,
                'size': len(response.content),
                'status': 'downloaded'
            }
        else:
            logger.warning(f"✗ Failed {org}: HTTP {response.status_code}")
            return {
                'success': False,
                'error': f'HTTP {response.status_code}',
                'status': 'failed'
            }
            
    except Exception as e:
        logger.error(f"✗ Error downloading {org}: {e}")
        return {
            'success': False,
            'error': str(e),
            'status': 'error'
        }

def main():
    """Download all page 2 policies"""
    results = []
    
    logger.info(f"Starting download of {len(page2_policies)} COI policies from page 2...")
    
    for policy in page2_policies:
        result = download_policy(policy['org'], policy['url'])
        result['organization'] = policy['org']
        result['url'] = policy['url']
        results.append(result)
        time.sleep(1)  # Be polite
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/page2_download_report_{timestamp}.json"
    os.makedirs("data/output", exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total': len(results),
            'successful': sum(1 for r in results if r['success']),
            'failed': sum(1 for r in results if not r['success']),
            'results': results
        }, f, indent=2)
    
    # Print summary
    successful = sum(1 for r in results if r['success'])
    print(f"\n{'='*60}")
    print(f"Downloaded {successful} of {len(results)} policies from page 2")
    print(f"Report saved to: {report_file}")
    
    for r in results:
        if r['success']:
            print(f"✓ {r['organization']}")

if __name__ == "__main__":
    main()