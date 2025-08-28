#!/usr/bin/env python3
"""
COI Policy Collector for US Healthcare Systems
Comprehensive tool for downloading Conflict of Interest policies
Project: DA-181 - COI Policy WebSearch
"""

import os
import json
import requests
import hashlib
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import urllib3

# Suppress SSL warnings for government sites
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class COIPolicyCollector:
    """Main collector class for COI policies"""
    
    def __init__(self, download_dir="data/raw/policies", output_dir="data/output"):
        self.download_dir = download_dir
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
    def download_policy(self, policy_data):
        """Download a single policy document with robust error handling"""
        org = policy_data['org']
        url = policy_data['url']
        
        try:
            # Create filename
            org_clean = org.replace(' ', '_').replace('/', '_').replace('&', 'and')
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            
            # Determine extension
            if '.pdf' in url.lower():
                ext = 'pdf'
            elif '.html' in url.lower() or '.aspx' in url.lower() or url.endswith('/'):
                ext = 'html'
            elif '.doc' in url.lower() or '.docx' in url.lower():
                ext = 'doc'
            else:
                ext = 'pdf'  # Default
                
            filename = f"{self.download_dir}/{org_clean}_{url_hash}.{ext}"
            
            # Check if already downloaded
            if os.path.exists(filename):
                logger.info(f"Already exists: {org}")
                return {
                    'success': True, 
                    'org': org, 
                    'filename': filename, 
                    'status': 'already_exists'
                }
            
            # Try different headers for government sites
            headers_list = [
                self.session.headers,
                {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                }
            ]
            
            # Try download with different headers
            for headers in headers_list:
                try:
                    response = requests.get(
                        url, 
                        headers=headers, 
                        timeout=30, 
                        allow_redirects=True, 
                        verify=False
                    )
                    
                    if response.status_code == 200:
                        os.makedirs(self.download_dir, exist_ok=True)
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        
                        logger.info(f"✓ Downloaded: {org}")
                        return {
                            'success': True,
                            'org': org,
                            'url': url,
                            'filename': filename,
                            'size': len(response.content),
                            'status': 'downloaded'
                        }
                    elif response.status_code == 403:
                        continue  # Try next header
                except:
                    continue
                    
            logger.warning(f"✗ Failed {org}: HTTP {response.status_code if 'response' in locals() else 'Error'}")
            return {
                'success': False,
                'org': org,
                'url': url,
                'error': f'HTTP {response.status_code if "response" in locals() else "Error"}',
                'status': 'failed'
            }
                
        except Exception as e:
            logger.error(f"✗ Error downloading {org}: {str(e)[:50]}")
            return {
                'success': False,
                'org': org,
                'url': url,
                'error': str(e)[:100],
                'status': 'error'
            }
    
    def collect_policies(self, policies, max_workers=5):
        """Download multiple policies concurrently"""
        results = []
        successful_downloads = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_policy = {
                executor.submit(self.download_policy, policy): policy 
                for policy in policies
            }
            
            for future in as_completed(future_to_policy):
                result = future.result()
                results.append(result)
                if result['success'] and result['status'] == 'downloaded':
                    successful_downloads.append(result['org'])
        
        return results, successful_downloads
    
    def save_report(self, results, collection_name="collection"):
        """Save collection report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"{self.output_dir}/{collection_name}_report_{timestamp}.json"
        os.makedirs(self.output_dir, exist_ok=True)
        
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'collection_name': collection_name,
            'total_attempted': len(results),
            'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
            'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
            'failed': len([r for r in results if not r['success']]),
            'results': sorted(results, key=lambda x: (not x['success'], x['org']))
        }
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        return report_file
    
    def print_summary(self, results, successful_downloads, report_file):
        """Print collection summary"""
        print(f"\n{'='*60}")
        print("COI POLICY COLLECTION COMPLETE")
        print(f"{'='*60}")
        print(f"Total policies attempted: {len(results)}")
        print(f"New downloads: {len([r for r in results if r['success'] and r['status'] == 'downloaded'])}")
        print(f"Already existed: {len([r for r in results if r['success'] and r['status'] == 'already_exists'])}")
        print(f"Failed downloads: {len([r for r in results if not r['success']])}")
        print(f"\nReport saved to: {report_file}")
        
        if successful_downloads:
            print(f"\n{'='*60}")
            print("Newly Downloaded Organizations:")
            print(f"{'='*60}")
            
            for org in successful_downloads[:30]:
                print(f"✓ {org}")
            
            if len(successful_downloads) > 30:
                print(f"... and {len(successful_downloads) - 30} more")


def get_healthcare_policies():
    """Get comprehensive list of US healthcare COI policy URLs"""
    return [
        # Major Health Systems
        {"org": "Mayo Clinic", "url": "https://mcforms.mayo.edu/mc0200-mc0299/mc0219-09.pdf"},
        {"org": "Cleveland Clinic", "url": "https://my.clevelandclinic.org/-/scassets/files/org/about/integrity-agreement.pdf"},
        {"org": "Johns Hopkins Medicine", "url": "https://www.hopkinsmedicine.org/research/resources/offices-policies/ora/policies/conflict_of_interest.html"},
        {"org": "Stanford Medicine", "url": "https://med.stanford.edu/coi/documents/stanford-som-coi-policy.pdf"},
        {"org": "Kaiser Permanente Research", "url": "https://www.kpwashingtonresearch.org/application/files/4815/5432/4781/KP_policy_on_conflicts_of_interest_in_research.pdf"},
        {"org": "Mount Sinai Health", "url": "https://www.mountsinai.org/files/MSHealth/Assets/HS/About/MSHS_CodeofConduct_revised_12.08.17.pdf"},
        
        # Children's Hospitals
        {"org": "Boston Children's Hospital", "url": "https://www.childrenshospital.org/sites/default/files/2024-03/irb-012-002-Conflict-Interest-Commitment-Policy-Procedure.pdf"},
        {"org": "Children's Hospital of Philadelphia", "url": "https://www.chop.edu/sites/default/files/conflict-of-interest-policy.pdf"},
        
        # Add more as needed...
    ]


def get_federal_state_policies():
    """Get federal and state healthcare agency COI policies"""
    return [
        # Federal Agencies
        {"org": "FDA Ethics", "url": "https://www.fda.gov/media/85072/download"},
        {"org": "NIH Ethics Office", "url": "https://oir.nih.gov/sites/default/files/uploads/sourcebook/documents/ethical_conduct/coi-guide-2021.pdf"},
        {"org": "CDC Ethics Office", "url": "https://www.cdc.gov/about/ethics/pdf/coi-policy.pdf"},
        {"org": "CMS Ethics Division", "url": "https://www.cms.gov/files/document/cms-conflict-interest-policy.pdf"},
        {"org": "Veterans Affairs", "url": "https://www.ethics.va.gov/docs/policy/policies_coi_20210826.pdf"},
        
        # State Health Departments
        {"org": "California Dept of Public Health", "url": "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/COI_Policy.pdf"},
        {"org": "New York State Dept of Health", "url": "https://www.health.ny.gov/funding/forms/attachment_10_coi.pdf"},
        {"org": "Texas Health and Human Services", "url": "https://www.hhs.texas.gov/sites/default/files/documents/laws-regulations/policies-rules/coi-policy.pdf"},
        
        # Add more as needed...
    ]


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Download COI policies from US healthcare systems'
    )
    parser.add_argument(
        '--collection', 
        default='all',
        choices=['all', 'healthcare', 'federal-state'],
        help='Which collection to download'
    )
    parser.add_argument(
        '--max-workers', 
        type=int, 
        default=5,
        help='Maximum concurrent downloads'
    )
    parser.add_argument(
        '--output-dir', 
        default='data/output',
        help='Output directory for reports'
    )
    parser.add_argument(
        '--download-dir', 
        default='data/raw/policies',
        help='Download directory for policies'
    )
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = COIPolicyCollector(
        download_dir=args.download_dir,
        output_dir=args.output_dir
    )
    
    # Get policies based on collection type
    policies = []
    if args.collection == 'healthcare':
        policies = get_healthcare_policies()
    elif args.collection == 'federal-state':
        policies = get_federal_state_policies()
    else:  # all
        policies = get_healthcare_policies() + get_federal_state_policies()
    
    logger.info(f"Starting collection of {len(policies)} COI policies...")
    
    # Collect policies
    results, successful_downloads = collector.collect_policies(
        policies, 
        args.max_workers
    )
    
    # Save report
    report_file = collector.save_report(results, args.collection)
    
    # Print summary
    collector.print_summary(results, successful_downloads, report_file)


if __name__ == "__main__":
    main()