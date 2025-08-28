#!/usr/bin/env python3
"""
Main COI Policy Collector for US Healthcare Systems
Consolidated script for downloading Conflict of Interest policies
Project: DA-181 - COI Policy WebSearch
"""

import os
import json
import requests
import hashlib
from datetime import datetime
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class COIPolicyCollector:
    """Main collector class for COI policies"""
    
    def __init__(self, download_dir="data/raw/policies", output_dir="data/output"):
        self.download_dir = download_dir
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def download_policy(self, policy_data):
        """Download a single policy document"""
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
            else:
                ext = 'pdf'  # Default
                
            filename = f"{self.download_dir}/{org_clean}_{url_hash}.{ext}"
            
            # Check if already downloaded
            if os.path.exists(filename):
                return {'success': True, 'org': org, 'filename': filename, 'status': 'already_exists'}
            
            # Download file
            response = self.session.get(url, timeout=30, allow_redirects=True, verify=False)
            
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
            else:
                logger.warning(f"✗ Failed {org}: HTTP {response.status_code}")
                return {
                    'success': False,
                    'org': org,
                    'url': url,
                    'error': f'HTTP {response.status_code}',
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
        
        # Use thread pool for faster downloads
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
        
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'collection_name': collection_name,
                'total_attempted': len(results),
                'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
                'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
                'failed': len([r for r in results if not r['success']]),
                'results': sorted(results, key=lambda x: (not x['success'], x['org']))
            }, f, indent=2)
        
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


def get_all_policies():
    """Get comprehensive list of all US healthcare COI policy URLs"""
    return [
        # Major Health Systems
        {"org": "Mayo Clinic", "url": "https://mcforms.mayo.edu/mc0200-mc0299/mc0219-09.pdf"},
        {"org": "Cleveland Clinic", "url": "https://my.clevelandclinic.org/-/scassets/files/org/about/integrity-agreement.pdf"},
        {"org": "Johns Hopkins Medicine", "url": "https://www.hopkinsmedicine.org/research/resources/offices-policies/ora/policies/conflict_of_interest.html"},
        {"org": "Stanford Medicine", "url": "https://med.stanford.edu/coi/documents/stanford-som-coi-policy.pdf"},
        {"org": "Mass General Brigham", "url": "https://www.massgeneralbrigham.org/notices/conflict-of-interest"},
        {"org": "UPMC", "url": "https://www.upmc.com/-/media/upmc/about/compliance/documents/conflictofinterestpolicy.pdf"},
        {"org": "NewYork-Presbyterian", "url": "https://www.nyp.org/documents/conflict-of-interest-policy.pdf"},
        {"org": "Cedars-Sinai", "url": "https://www.cedars-sinai.org/content/dam/cedars-sinai/research/documents/irb/conflict-of-interest-policy.pdf"},
        {"org": "Northwestern Medicine", "url": "https://www.northwestern.edu/coi/policy/index.html"},
        {"org": "Mount Sinai Health", "url": "https://icahn.mssm.edu/files/ISMMS/Assets/Research/COI/coi_policy.pdf"},
        {"org": "NYU Langone", "url": "https://med.nyu.edu/research/office-science-research/conflict-interest"},
        {"org": "Duke Health", "url": "https://policies.duke.edu/sites/default/files/2020-03/COI_Policy.pdf"},
        {"org": "Vanderbilt Medical", "url": "https://www.vanderbilt.edu/generalcounsel/files/COI_Policy.pdf"},
        {"org": "Emory Healthcare", "url": "https://www.emoryhealthcare.org/ui/pdfs/vendor-coi-policy.pdf"},
        {"org": "Michigan Medicine", "url": "https://www.uofmhealth.org/provider/policies/conflict-of-interest"},
        {"org": "Penn Medicine", "url": "https://www.med.upenn.edu/coi/assets/user-content/documents/coi-policy.pdf"},
        {"org": "UCSF Medical", "url": "https://policies.ucsf.edu/policy/100-20"},
        {"org": "UCLA Medical", "url": "https://medschool.ucla.edu/sites/default/files/site/coi-policy-2020.pdf"},
        {"org": "Yale New Haven Health", "url": "https://medicine.yale.edu/research/human-research/resources/policies/conflictofinterest/"},
        {"org": "Columbia University Medical", "url": "https://www.cuimc.columbia.edu/research/conflict-interest-and-research"},
        
        # Children's Hospitals
        {"org": "Boston Children's Hospital", "url": "https://www.childrenshospital.org/sites/default/files/2024-03/irb-012-002-Conflict-Interest-Commitment-Policy-Procedure.pdf"},
        {"org": "Children's Hospital of Philadelphia", "url": "https://www.chop.edu/sites/default/files/conflict-of-interest-policy.pdf"},
        {"org": "Texas Children's Hospital", "url": "https://www.texaschildrens.org/sites/default/files/conflict-of-interest-policy.pdf"},
        {"org": "Cincinnati Children's", "url": "https://www.cincinnatichildrens.org/-/media/cincinnati-childrens/home/research/compliance/conflict-of-interest-policy.pdf"},
        {"org": "Children's Healthcare of Atlanta", "url": "https://www.choa.org/-/media/files/choa/documents/compliance/conflict-of-interest-policy.pdf"},
        {"org": "Seattle Children's", "url": "https://www.seattlechildrens.org/globalassets/documents/healthcare-professionals/conflict-of-interest-policy.pdf"},
        {"org": "Nationwide Children's Hospital", "url": "https://www.nationwidechildrens.org/-/media/nch/documents/compliance/conflict-of-interest-policy.pdf"},
        {"org": "Children's Hospital Los Angeles", "url": "https://www.chla.org/sites/default/files/atoms/files/conflict-of-interest-policy.pdf"},
        {"org": "Children's Hospital Colorado", "url": "https://www.childrenscolorado.org/globalassets/documents/healthcare-professionals/conflict-of-interest-policy.pdf"},
        {"org": "Children's Mercy", "url": "https://www.childrensmercy.org/contentassets/760a942216154ee5bcb749a662e7e22c/com-conflict-of-interest-policy.docx.pdf"},
        {"org": "Stanley Manne Children's Research Institute", "url": "https://research.luriechildrens.org/globalassets/stanley-manne-research-site/research-resources/office-of-research-integrity-and-compliance/conflicts-of-interest-and-commitment-in-research-policy.pdf"},
        {"org": "Nemours Children's Health", "url": "https://www.nemours.org/content/dam/reading-brightstart/documents/coipolicy.pdf"},
        {"org": "Children's National Hospital", "url": "https://childrensnational.org/-/media/cnhs-site/files/research-and-education/conflict-of-interest-policy.pdf?la=en"},
        
        # Add more as needed...
    ]


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Download COI policies from US healthcare systems')
    parser.add_argument('--collection', default='all', choices=['all', 'major', 'children', 'academic'],
                        help='Which collection to download')
    parser.add_argument('--max-workers', type=int, default=5, 
                        help='Maximum concurrent downloads')
    parser.add_argument('--output-dir', default='data/output',
                        help='Output directory for reports')
    parser.add_argument('--download-dir', default='data/raw/policies',
                        help='Download directory for policies')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = COIPolicyCollector(
        download_dir=args.download_dir,
        output_dir=args.output_dir
    )
    
    # Get policies to collect
    policies = get_all_policies()
    
    # Filter by collection type if specified
    if args.collection == 'major':
        policies = [p for p in policies if 'Children' not in p['org']]
    elif args.collection == 'children':
        policies = [p for p in policies if 'Children' in p['org']]
    elif args.collection == 'academic':
        policies = [p for p in policies if any(word in p['org'] for word in ['University', 'UCLA', 'UCSF', 'NYU', 'Penn'])]
    
    logger.info(f"Starting collection of {len(policies)} COI policies...")
    
    # Collect policies
    results, successful_downloads = collector.collect_policies(policies, args.max_workers)
    
    # Save report
    report_file = collector.save_report(results, args.collection)
    
    # Print summary
    collector.print_summary(results, successful_downloads, report_file)


if __name__ == "__main__":
    main()