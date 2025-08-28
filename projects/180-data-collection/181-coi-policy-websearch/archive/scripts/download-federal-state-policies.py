#!/usr/bin/env python3
"""
Download COI policies from federal and state healthcare agencies
Focus on US government healthcare organizations
"""

import os
import json
import requests
import hashlib
from datetime import datetime
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Federal and State Healthcare COI Policies
federal_state_policies = [
    # Federal Agencies
    {"org": "HHS Office of Inspector General", "url": "https://oig.hhs.gov/documents/root/162/coi_policy.pdf"},
    {"org": "CDC Ethics Office", "url": "https://www.cdc.gov/about/ethics/pdf/coi-policy.pdf"},
    {"org": "FDA Ethics", "url": "https://www.fda.gov/media/85072/download"},
    {"org": "NIH Ethics Office", "url": "https://oir.nih.gov/sites/default/files/uploads/sourcebook/documents/ethical_conduct/coi-guide-2021.pdf"},
    {"org": "CMS Ethics Division", "url": "https://www.cms.gov/files/document/cms-conflict-interest-policy.pdf"},
    {"org": "HRSA Bureau of Health Workforce", "url": "https://bhw.hrsa.gov/sites/default/files/bureau-health-workforce/funding/coi-policy.pdf"},
    {"org": "SAMHSA", "url": "https://www.samhsa.gov/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Indian Health Service", "url": "https://www.ihs.gov/sites/omc/themes/responsive2017/display_objects/documents/COI_Policy.pdf"},
    {"org": "AHRQ", "url": "https://www.ahrq.gov/sites/default/files/publications/files/coi-policy.pdf"},
    {"org": "Veterans Affairs", "url": "https://www.ethics.va.gov/docs/policy/policies_coi_20210826.pdf"},
    {"org": "DOD Military Health System", "url": "https://health.mil/Reference-Center/Policies/2021/08/20/Conflict-of-Interest-Policy"},
    {"org": "Public Health Service", "url": "https://www.hhs.gov/sites/default/files/financial-conflict-of-interest.pdf"},
    {"org": "National Science Foundation", "url": "https://www.nsf.gov/pubs/policydocs/pappguide/nsf23001/aag_4.pdf"},
    {"org": "PCORI", "url": "https://www.pcori.org/sites/default/files/PCORI-Conflict-of-Interest-Policy.pdf"},
    {"org": "Health Resources Services Admin", "url": "https://www.hrsa.gov/sites/default/files/hrsa/grants/manage/technicalassistance/conflict-of-interest.pdf"},
    
    # State Health Departments - Major States
    {"org": "California Dept of Public Health", "url": "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/COI_Policy.pdf"},
    {"org": "New York State Dept of Health", "url": "https://www.health.ny.gov/funding/forms/attachment_10_coi.pdf"},
    {"org": "Texas Health and Human Services", "url": "https://www.hhs.texas.gov/sites/default/files/documents/laws-regulations/policies-rules/coi-policy.pdf"},
    {"org": "Florida Dept of Health", "url": "http://www.floridahealth.gov/about/administrative-functions/general-counsel/_documents/conflict-of-interest-policy.pdf"},
    {"org": "Illinois Dept of Public Health", "url": "https://dph.illinois.gov/content/dam/soi/en/web/idph/files/conflict-of-interest-policy.pdf"},
    {"org": "Pennsylvania Dept of Health", "url": "https://www.health.pa.gov/topics/Documents/Administrative/Conflict-of-Interest-Policy.pdf"},
    {"org": "Ohio Dept of Health", "url": "https://odh.ohio.gov/wps/wcm/connect/gov/conflict-of-interest-policy.pdf"},
    {"org": "Michigan Dept of Health", "url": "https://www.michigan.gov/mdhhs/-/media/Project/Websites/mdhhs/Folder1/Folder52/COI_Policy.pdf"},
    {"org": "North Carolina DHHS", "url": "https://www.ncdhhs.gov/media/783/download"},
    {"org": "Georgia Dept of Public Health", "url": "https://dph.georgia.gov/document/publication/conflict-interest-policy/download"},
    {"org": "Virginia Dept of Health", "url": "https://www.vdh.virginia.gov/content/uploads/sites/191/2020/09/VDH-COI-Policy.pdf"},
    {"org": "Washington State Dept of Health", "url": "https://www.doh.wa.gov/Portals/1/Documents/1000/COI-Policy.pdf"},
    {"org": "Massachusetts Dept of Public Health", "url": "https://www.mass.gov/doc/conflict-of-interest-policy-0/download"},
    {"org": "New Jersey Dept of Health", "url": "https://www.nj.gov/health/legal/documents/conflict-of-interest.pdf"},
    {"org": "Arizona Dept of Health Services", "url": "https://www.azdhs.gov/documents/operations/conflict-of-interest-policy.pdf"},
    {"org": "Tennessee Dept of Health", "url": "https://www.tn.gov/content/dam/tn/health/documents/conflict-of-interest-policy.pdf"},
    {"org": "Indiana State Dept of Health", "url": "https://www.in.gov/health/files/Conflict-of-Interest-Policy.pdf"},
    {"org": "Missouri Dept of Health", "url": "https://health.mo.gov/about/pdf/conflict-of-interest-policy.pdf"},
    {"org": "Maryland Dept of Health", "url": "https://health.maryland.gov/docs/COI_Policy.pdf"},
    {"org": "Wisconsin Dept of Health Services", "url": "https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf"},
    {"org": "Minnesota Dept of Health", "url": "https://www.health.state.mn.us/about/org/conflict-interest-policy.pdf"},
    {"org": "Colorado Dept of Public Health", "url": "https://cdphe.colorado.gov/sites/cdphe/files/CHED_COI_Policy.pdf"},
    {"org": "Alabama Dept of Public Health", "url": "https://www.alabamapublichealth.gov/legal/assets/coi-policy.pdf"},
    {"org": "South Carolina DHEC", "url": "https://scdhec.gov/sites/default/files/Library/CR-012148.pdf"},
    {"org": "Louisiana Dept of Health", "url": "https://ldh.la.gov/assets/docs/BayouHealth/ConflictofInterest.pdf"},
    {"org": "Kentucky Cabinet for Health", "url": "https://chfs.ky.gov/agencies/os/oig/Documents/ConflictofInterestPolicy.pdf"},
    {"org": "Oregon Health Authority", "url": "https://www.oregon.gov/oha/HPA/HP-PCO/Documents/COI-Policy.pdf"},
    {"org": "Connecticut Dept of Public Health", "url": "https://portal.ct.gov/-/media/Departments-and-Agencies/DPH/dph/state_health_planning/SHA-SHIP/HCCDocuments/COI_Policy.pdf"},
    {"org": "Iowa Dept of Public Health", "url": "https://idph.iowa.gov/Portals/1/userfiles/91/Conflict%20of%20Interest%20Policy.pdf"},
    {"org": "Oklahoma State Dept of Health", "url": "https://oklahoma.gov/content/dam/ok/en/health/health2/documents/conflict-of-interest-policy.pdf"},
    {"org": "Arkansas Dept of Health", "url": "https://www.healthy.arkansas.gov/images/uploads/pdf/ConflictofInterestPolicy.pdf"},
    {"org": "Utah Dept of Health", "url": "https://health.utah.gov/wp-content/uploads/COI-Policy.pdf"},
    {"org": "Kansas Dept of Health", "url": "https://www.kdhe.ks.gov/DocumentCenter/View/124/Conflict-of-Interest-Policy-PDF"},
    {"org": "Nevada Dept of Health", "url": "https://dhhs.nv.gov/uploadedFiles/dhhs.nv.gov/content/Resources/AdminSupport/COI-Policy.pdf"},
    {"org": "New Mexico Dept of Health", "url": "https://www.nmhealth.org/publication/view/policy/6093/"},
    {"org": "Mississippi State Dept of Health", "url": "https://msdh.ms.gov/msdhsite/_static/resources/78.pdf"},
    {"org": "West Virginia DHHR", "url": "https://dhhr.wv.gov/bms/Documents/COI_Policy.pdf"},
    {"org": "Hawaii Dept of Health", "url": "https://health.hawaii.gov/opppd/files/2019/08/COI-Policy.pdf"},
    {"org": "Maine CDC", "url": "https://www.maine.gov/dhhs/mecdc/public-health-systems/coi-policy.pdf"},
    {"org": "Nebraska DHHS", "url": "http://dhhs.ne.gov/Documents/COI-Policy.pdf"},
    {"org": "Idaho Dept of Health", "url": "https://healthandwelfare.idaho.gov/sites/default/files/2019-11/COI_Policy.pdf"},
    {"org": "Rhode Island Dept of Health", "url": "https://health.ri.gov/publications/policies/ConflictOfInterest.pdf"},
    {"org": "Montana DPHHS", "url": "https://dphhs.mt.gov/assets/publichealth/CDEpi/COI_Policy.pdf"},
    {"org": "Delaware Health and Social Services", "url": "https://www.dhss.delaware.gov/dhss/admin/files/coi_policy.pdf"},
    {"org": "North Dakota Dept of Health", "url": "https://www.ndhealth.gov/Files/COI_Policy.pdf"},
    {"org": "South Dakota Dept of Health", "url": "https://doh.sd.gov/documents/COI-Policy.pdf"},
    {"org": "Vermont Dept of Health", "url": "https://www.healthvermont.gov/sites/default/files/documents/pdf/COI_Policy.pdf"},
    {"org": "Alaska DHSS", "url": "https://dhss.alaska.gov/Commissioner/Documents/COI-policy.pdf"},
    {"org": "Wyoming Dept of Health", "url": "https://health.wyo.gov/wp-content/uploads/2020/10/COI-Policy.pdf"},
    {"org": "DC Health", "url": "https://dchealth.dc.gov/sites/default/files/dc/sites/doh/page_content/attachments/Conflict%20of%20Interest%20Policy.pdf"}
]

def download_policy(policy_data, download_dir="data/raw/policies"):
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
            
        filename = f"{download_dir}/{org_clean}_{url_hash}.{ext}"
        
        # Check if already downloaded
        if os.path.exists(filename):
            return {'success': True, 'org': org, 'filename': filename, 'status': 'already_exists'}
        
        # Download file
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True, verify=False)
        
        if response.status_code == 200:
            os.makedirs(download_dir, exist_ok=True)
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

def main():
    """Download all federal and state COI policies"""
    logger.info(f"Starting download of {len(federal_state_policies)} federal and state COI policies...")
    
    results = []
    successful_downloads = []
    
    # Use thread pool for faster downloads
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_policy = {
            executor.submit(download_policy, policy): policy 
            for policy in federal_state_policies
        }
        
        for future in as_completed(future_to_policy):
            result = future.result()
            results.append(result)
            if result['success'] and result['status'] == 'downloaded':
                successful_downloads.append(result['org'])
    
    # Sort results by success
    results.sort(key=lambda x: (not x['success'], x['org']))
    
    # Save comprehensive report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/federal_state_download_{timestamp}.json"
    os.makedirs("data/output", exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_attempted': len(federal_state_policies),
            'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
            'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
            'failed': len([r for r in results if not r['success']]),
            'results': results
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print("FEDERAL & STATE COI POLICY DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total policies attempted: {len(federal_state_policies)}")
    print(f"New downloads: {len([r for r in results if r['success'] and r['status'] == 'downloaded'])}")
    print(f"Already existed: {len([r for r in results if r['success'] and r['status'] == 'already_exists'])}")
    print(f"Failed downloads: {len([r for r in results if not r['success']])}")
    print(f"\nReport saved to: {report_file}")
    
    if successful_downloads:
        print(f"\n{'='*60}")
        print("Successfully Downloaded Organizations:")
        print(f"{'='*60}")
        
        # Show federal agencies first
        federal = [org for org in successful_downloads if any(word in org for word in ['HHS', 'CDC', 'FDA', 'NIH', 'CMS', 'HRSA', 'SAMHSA', 'Veterans', 'DOD', 'AHRQ', 'PCORI', 'Indian Health', 'Public Health Service', 'National Science'])]
        if federal:
            print("\nFEDERAL AGENCIES:")
            for org in federal:
                print(f"✓ {org}")
        
        # Show state departments
        state = [org for org in successful_downloads if org not in federal]
        if state:
            print("\nSTATE HEALTH DEPARTMENTS:")
            for org in state[:20]:  # Show first 20 states
                print(f"✓ {org}")
            if len(state) > 20:
                print(f"... and {len(state) - 20} more states")

if __name__ == "__main__":
    main()