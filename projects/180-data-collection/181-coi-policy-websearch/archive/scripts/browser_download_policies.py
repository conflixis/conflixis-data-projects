#!/usr/bin/env python3
"""
Download COI policies found in browser search
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

# URLs found from Google search
coi_policy_urls = [
    {"org": "Boston Children's Hospital", "url": "https://www.childrenshospital.org/sites/default/files/2024-03/irb-012-002-Conflict-Interest-Commitment-Policy-Procedure.pdf"},
    {"org": "Children's Mercy", "url": "https://www.childrensmercy.org/contentassets/760a942216154ee5bcb749a662e7e22c/com-conflict-of-interest-policy.docx.pdf"},
    {"org": "Stanley Manne Children's Research Institute", "url": "https://research.luriechildrens.org/globalassets/stanley-manne-research-site/research-resources/office-of-research-integrity-and-compliance/conflicts-of-interest-and-commitment-in-research-policy.pdf"},
    {"org": "Mayo Clinic", "url": "https://mcforms.mayo.edu/mc0200-mc0299/mc0219-09.pdf"},
    {"org": "Atrium Health", "url": "https://atriumhealth.org/documents/fcoi-chs-cor-40-17.pdf"},
    {"org": "American College of Surgeons", "url": "https://www.facs.org/media/jrvb4jvr/policy.pdf"},
    {"org": "Unity Health Toronto", "url": "https://unityhealth.to/wp-content/uploads/2023/07/Relationship-Disclosure-and-Management-Policy.pdf"},
    {"org": "Memorial Sloan Kettering", "url": "https://www.mskcc.org/teaser/conflict-interest-commitment-policy.pdf"},
    {"org": "Hospital for Special Surgery", "url": "https://www.hss.edu/contentassets/4abd50ba30fa4c5687615baee72cc172/hss-conflict-of-interest-policy-appendix.pdf"},
    {"org": "Osivax", "url": "https://osivax.com/wp-content/uploads/2023/07/230712-Conflict-of-interest.pdf"},
]

def download_policy(org, url, download_dir="data/raw/policies"):
    """Download a single policy PDF"""
    os.makedirs(download_dir, exist_ok=True)
    
    try:
        # Create filename
        org_clean = org.replace(' ', '_').replace('/', '_')
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{download_dir}/{org_clean}_{url_hash}.pdf"
        
        # Check if already downloaded
        if os.path.exists(filename):
            logger.info(f"Already downloaded: {filename}")
            return {'success': True, 'filename': filename, 'status': 'already_exists'}
        
        # Download file
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        
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
    """Download all policies"""
    results = []
    
    logger.info(f"Starting download of {len(coi_policy_urls)} COI policies...")
    
    for policy in coi_policy_urls:
        result = download_policy(policy['org'], policy['url'])
        result['organization'] = policy['org']
        result['url'] = policy['url']
        results.append(result)
        time.sleep(1)  # Be polite
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/browser_download_report_{timestamp}.json"
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
    print(f"Downloaded {successful} of {len(results)} policies")
    print(f"Report saved to: {report_file}")
    
    for r in results:
        if r['success']:
            print(f"✓ {r['organization']}")

if __name__ == "__main__":
    main()