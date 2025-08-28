#!/usr/bin/env python3
"""
Quick COI Policy Finder and Downloader for US Healthcare Systems
Downloads actual policy documents (PDFs, HTML) from known healthcare sites
"""

import os
import json
import time
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class QuickCOIDownloader:
    """Fast COI policy finder and downloader"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.found_policies = []
        self.download_dir = "data/raw/policies"
        os.makedirs(self.download_dir, exist_ok=True)
        
    def get_known_coi_urls(self):
        """Return list of known/likely COI policy URLs for major US healthcare systems"""
        return [
            # Mayo Clinic
            {"org": "Mayo Clinic", "url": "https://www.mayo.edu/research/documents/conflict-of-interest-policy/doc-10026934"},
            {"org": "Mayo Clinic", "url": "https://www.mayoclinic.org/documents/conflict-of-interest-policy/doc-20146569"},
            
            # Cleveland Clinic
            {"org": "Cleveland Clinic", "url": "https://my.clevelandclinic.org/about/community/ethics-integrity/conflict-of-interest"},
            {"org": "Cleveland Clinic", "url": "https://my.clevelandclinic.org/-/scassets/files/org/about/integrity-agreement.pdf"},
            
            # Johns Hopkins
            {"org": "Johns Hopkins", "url": "https://www.hopkinsmedicine.org/research/resources/offices-policies/ora/policies/conflict_of_interest.html"},
            {"org": "Johns Hopkins", "url": "https://www.jhu.edu/assets/uploads/2014/09/conflict_of_interest_policies.pdf"},
            
            # Stanford
            {"org": "Stanford Medicine", "url": "https://med.stanford.edu/coi/documents/stanford-som-coi-policy.pdf"},
            {"org": "Stanford Medicine", "url": "https://doresearch.stanford.edu/policies/research-policy-handbook/conflicts-commitment-and-interest/conflict-interest-policy"},
            
            # Harvard Medical
            {"org": "Harvard Medical", "url": "https://hms.harvard.edu/sites/default/files/assets/Sites/Integrity/files/HMS_COI_Policy.pdf"},
            {"org": "Harvard Medical", "url": "https://www.brighamandwomens.org/assets/BWH/about-bwh/pdfs/conflict-of-interest-disclosure-policy.pdf"},
            
            # UCSF
            {"org": "UCSF", "url": "https://policies.ucsf.edu/policy/100-20"},
            {"org": "UCSF", "url": "https://coi.ucsf.edu/sites/g/files/tkssra5056/f/wysiwyg/COI%20Policy.pdf"},
            
            # UCLA
            {"org": "UCLA Health", "url": "https://www.uclahealth.org/compliance/conflict-of-interest"},
            {"org": "UCLA Health", "url": "https://medschool.ucla.edu/sites/default/files/site/coi-policy-2020.pdf"},
            
            # Mount Sinai
            {"org": "Mount Sinai", "url": "https://icahn.mssm.edu/files/ISMMS/Assets/Research/COI/coi_policy.pdf"},
            {"org": "Mount Sinai", "url": "https://www.mountsinai.org/about/compliance/conflict-of-interest"},
            
            # NYU Langone
            {"org": "NYU Langone", "url": "https://med.nyu.edu/research/office-science-research/conflict-interest"},
            {"org": "NYU Langone", "url": "https://nyulangone.org/policies"},
            
            # Mass General Brigham
            {"org": "Mass General Brigham", "url": "https://www.massgeneralbrigham.org/notices/conflict-of-interest"},
            {"org": "Mass General Brigham", "url": "https://www.brighamandwomens.org/about-bwh/vendor-relations/conflict-of-interest-policy"},
            
            # Penn Medicine
            {"org": "Penn Medicine", "url": "https://www.med.upenn.edu/coi/assets/user-content/documents/coi-policy.pdf"},
            {"org": "Penn Medicine", "url": "https://www.pennmedicine.org/about/compliance-and-privacy/conflict-of-interest"},
            
            # Michigan Medicine
            {"org": "Michigan Medicine", "url": "https://www.uofmhealth.org/provider/policies/conflict-of-interest"},
            {"org": "Michigan Medicine", "url": "https://research-compliance.umich.edu/conflict-interest-coi"},
            
            # Duke
            {"org": "Duke Health", "url": "https://medschool.duke.edu/research/clinical-and-translational-research/duke-office-clinical-research/toolkit-5"},
            {"org": "Duke Health", "url": "https://policies.duke.edu/sites/default/files/2020-03/COI_Policy.pdf"},
            
            # Vanderbilt
            {"org": "Vanderbilt", "url": "https://www.vumc.org/compliance/conflict-interest"},
            {"org": "Vanderbilt", "url": "https://www.vanderbilt.edu/generalcounsel/files/COI_Policy.pdf"},
            
            # UPMC
            {"org": "UPMC", "url": "https://www.upmc.com/about/suppliers/conflict-of-interest"},
            {"org": "UPMC", "url": "https://www.upmc.com/-/media/upmc/about/compliance/documents/conflictofinterestpolicy.pdf"},
            
            # Cedars-Sinai
            {"org": "Cedars-Sinai", "url": "https://www.cedars-sinai.org/content/dam/cedars-sinai/research/documents/irb/conflict-of-interest-policy.pdf"},
            {"org": "Cedars-Sinai", "url": "https://www.cedars-sinai.org/research/administration/policies.html"},
            
            # Northwestern
            {"org": "Northwestern Medicine", "url": "https://www.nm.org/about-us/corporate-information/compliance"},
            {"org": "Northwestern Medicine", "url": "https://www.northwestern.edu/coi/policy/index.html"},
            
            # Emory
            {"org": "Emory Healthcare", "url": "https://www.emoryhealthcare.org/ui/pdfs/vendor-coi-policy.pdf"},
            {"org": "Emory Healthcare", "url": "http://policies.emory.edu/7.7"},
            
            # Houston Methodist
            {"org": "Houston Methodist", "url": "https://www.houstonmethodist.org/-/media/pdf/for-health-professionals/hmri/coi-policy.pdf"},
            {"org": "Houston Methodist", "url": "https://www.houstonmethodist.org/for-health-professionals/research/resources/"},
            
            # Kaiser Permanente
            {"org": "Kaiser Permanente", "url": "https://healthy.kaiserpermanente.org/content/dam/kporg/final/documents/directories/codes-of-conduct/principles-responsibilities-medical-group-en.pdf"},
            {"org": "Kaiser Permanente", "url": "https://about.kaiserpermanente.org/our-story/our-company/compliance-and-integrity"},
            
            # CommonSpirit Health
            {"org": "CommonSpirit Health", "url": "https://www.commonspirit.org/content/dam/commonspirit/pdfs/Conflict-of-Interest-Policy.pdf"},
            {"org": "CommonSpirit Health", "url": "https://home.catholichealth.net/vendor-resources/conflict-of-interest"},
            
            # Ascension
            {"org": "Ascension", "url": "https://healthcare.ascension.org/-/media/healthcare/compliance/conflict-of-interest-policy.pdf"},
            {"org": "Ascension", "url": "https://www.ascension.org/About/Compliance"},
            
            # Providence
            {"org": "Providence", "url": "https://www.providence.org/about/compliance/conflict-of-interest"},
            {"org": "Providence", "url": "https://www.providence.org/-/media/Project/psjh/providence/socal/Files/about/compliance/coi-policy.pdf"},
            
            # HCA Healthcare
            {"org": "HCA Healthcare", "url": "https://hcahealthcare.com/ethics/"},
            {"org": "HCA Healthcare", "url": "https://hcahealthcare.com/util/documents/ethics/conflict-of-interest-policy.pdf"},
            
            # Baylor Scott & White
            {"org": "Baylor Scott & White", "url": "https://www.bswhealth.com/SiteCollectionDocuments/about/compliance/conflict-of-interest-policy.pdf"},
            {"org": "Baylor Scott & White", "url": "https://www.bswhealth.com/about/compliance"},
            
            # Intermountain
            {"org": "Intermountain Healthcare", "url": "https://intermountainhealthcare.org/about/who-we-are/trustee-resource-center/conflicts-of-interest/"},
            {"org": "Intermountain Healthcare", "url": "https://intermountainhealthcare.org/-/media/files/conflict-of-interest-policy.pdf"},
            
            # Sutter Health
            {"org": "Sutter Health", "url": "https://www.sutterhealth.org/about/compliance/conflict-of-interest"},
            {"org": "Sutter Health", "url": "https://www.sutterhealth.org/pdf/vendors/conflict-of-interest-policy.pdf"},
            
            # Banner Health
            {"org": "Banner Health", "url": "https://www.bannerhealth.com/about/compliance/conflict-of-interest"},
            {"org": "Banner Health", "url": "https://www.bannerhealth.com/-/media/files/project/bh/coi-policy.pdf"},
            
            # Atrium Health
            {"org": "Atrium Health", "url": "https://atriumhealth.org/about-us/compliance-and-privacy/conflict-of-interest"},
            {"org": "Atrium Health", "url": "https://atriumhealth.org/-/media/atrium/documents/coi-policy.pdf"},
            
            # Yale New Haven
            {"org": "Yale New Haven", "url": "https://www.ynhhs.org/about/compliance/conflict-of-interest.aspx"},
            {"org": "Yale New Haven", "url": "https://medicine.yale.edu/research/human-research/resources/policies/conflictofinterest/"},
            
            # Government/CMS
            {"org": "CMS", "url": "https://www.cms.gov/OpenPayments/Downloads/open-payments-general-fact-sheet.pdf"},
            {"org": "CMS", "url": "https://www.cms.gov/regulations-and-guidance/legislation/physician-payment-sunshine-act"},
            
            # Additional academic centers
            {"org": "Columbia", "url": "https://www.cuimc.columbia.edu/research/conflict-interest-and-research"},
            {"org": "UCSD", "url": "https://blink.ucsd.edu/research/compliance/conflict-of-interest/index.html"},
            {"org": "Washington University", "url": "https://research.wustl.edu/conflict-of-interest-financial-disclosure/"},
            {"org": "UNC", "url": "https://research.unc.edu/human-research-ethics/researchers/conflict-of-interest/"},
            {"org": "Pittsburgh", "url": "https://www.coi.pitt.edu/sites/default/files/documents/CoI%20Policy.pdf"},
        ]
    
    def download_document(self, policy_info):
        """Download a single policy document"""
        try:
            url = policy_info['url']
            org = policy_info['org']
            
            # Create filename
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            org_clean = org.replace(' ', '_').replace('/', '_')
            
            response = self.session.get(url, timeout=30, allow_redirects=True)
            
            if response.status_code == 200:
                # Determine file type
                content_type = response.headers.get('content-type', '').lower()
                
                if 'pdf' in content_type or url.endswith('.pdf'):
                    ext = 'pdf'
                elif 'html' in content_type or 'text' in content_type:
                    ext = 'html'
                else:
                    ext = 'dat'
                
                filename = f"{self.download_dir}/{org_clean}_{url_hash}.{ext}"
                
                # Save file
                with open(filename, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Downloaded: {org} - {filename}")
                
                return {
                    'organization': org,
                    'url': url,
                    'filename': filename,
                    'size': len(response.content),
                    'content_type': content_type,
                    'status': 'success',
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.warning(f"Failed to download {org}: HTTP {response.status_code}")
                return {
                    'organization': org,
                    'url': url,
                    'status': 'failed',
                    'error': f"HTTP {response.status_code}",
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error downloading {policy_info['org']}: {str(e)}")
            return {
                'organization': policy_info['org'],
                'url': policy_info['url'],
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def download_all_policies(self):
        """Download all known COI policies in parallel"""
        policies = self.get_known_coi_urls()
        results = []
        
        logger.info(f"Starting download of {len(policies)} COI policies...")
        
        # Use thread pool for parallel downloads
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_policy = {executor.submit(self.download_document, policy): policy 
                               for policy in policies}
            
            for future in as_completed(future_to_policy):
                result = future.result()
                results.append(result)
                
                # Show progress
                if result['status'] == 'success':
                    logger.info(f"✓ Downloaded {result['organization']}")
                else:
                    logger.warning(f"✗ Failed {result['organization']}")
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"data/output/download_report_{timestamp}.json"
        os.makedirs("data/output", exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump({
                'download_timestamp': datetime.now().isoformat(),
                'total_urls': len(policies),
                'successful': sum(1 for r in results if r['status'] == 'success'),
                'failed': sum(1 for r in results if r['status'] != 'success'),
                'results': results
            }, f, indent=2)
        
        return results, report_file

def main():
    """Main execution"""
    downloader = QuickCOIDownloader()
    results, report_file = downloader.download_all_policies()
    
    # Print summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = len(results) - successful
    
    print(f"\n{'='*60}")
    print("COI Policy Download Complete")
    print(f"{'='*60}")
    print(f"Total policies attempted: {len(results)}")
    print(f"Successfully downloaded: {successful}")
    print(f"Failed downloads: {failed}")
    print(f"\nDownloaded files saved to: data/raw/policies/")
    print(f"Download report saved to: {report_file}")
    
    # List successful downloads
    print(f"\n{'='*60}")
    print("Successfully Downloaded Policies:")
    print(f"{'='*60}")
    for r in results:
        if r['status'] == 'success':
            print(f"✓ {r['organization']}: {r['filename']}")

if __name__ == "__main__":
    main()