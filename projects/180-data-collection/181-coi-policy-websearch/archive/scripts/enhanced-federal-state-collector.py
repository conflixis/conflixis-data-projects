#!/usr/bin/env python3
"""
Enhanced Federal and State COI Policy Collector
Tries multiple URL patterns and search strategies to maximize collection
"""

import os
import json
import requests
import hashlib
from datetime import datetime
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Expanded list with alternative URLs and additional agencies
enhanced_federal_state_policies = [
    # FEDERAL AGENCIES - Primary URLs
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
    
    # FEDERAL AGENCIES - Alternative URLs
    {"org": "CDC Ethics Office Alt", "url": "https://www.cdc.gov/about/organization/ethics/pdf/coi-guidance.pdf"},
    {"org": "NIH Clinical Center", "url": "https://clinicalcenter.nih.gov/pdf/coi-policy.pdf"},
    {"org": "FDA CDER", "url": "https://www.fda.gov/media/85073/download"},
    {"org": "HHS General", "url": "https://www.hhs.gov/about/agencies/oga/about-oga/ethics/financial-disclosure/index.html"},
    {"org": "VA Research", "url": "https://www.research.va.gov/resources/policies/coi.pdf"},
    {"org": "USAID Global Health", "url": "https://www.usaid.gov/sites/default/files/documents/1864/303maa.pdf"},
    {"org": "EPA Office of Research", "url": "https://www.epa.gov/sites/default/files/2018-03/documents/coi_policy_2018.pdf"},
    {"org": "NOAA Research", "url": "https://nrc.noaa.gov/LinkClick.aspx?fileticket=qMZKIYV1pPo%3D&tabid=140"},
    {"org": "USDA Food Safety", "url": "https://www.fsis.usda.gov/sites/default/files/media_file/2021-02/COI-Policy.pdf"},
    {"org": "DOE Office of Science", "url": "https://science.osti.gov/-/media/grants/pdf/COI-Policy.pdf"},
    
    # STATE HEALTH DEPARTMENTS - All 50 States + DC with alternatives
    {"org": "California Dept of Public Health", "url": "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/COI_Policy.pdf"},
    {"org": "California Alt", "url": "https://www.dhcs.ca.gov/formsandpubs/Documents/MMCDAPLsandPolicyLetters/APL2017/APL17-010.pdf"},
    {"org": "New York State Dept of Health", "url": "https://www.health.ny.gov/funding/forms/attachment_10_coi.pdf"},
    {"org": "New York Alt", "url": "https://www.health.ny.gov/regulations/recently_adopted/docs/2011-02-23_conflict_of_interest.pdf"},
    {"org": "Texas Health and Human Services", "url": "https://www.hhs.texas.gov/sites/default/files/documents/laws-regulations/policies-rules/coi-policy.pdf"},
    {"org": "Texas Alt", "url": "https://www.dshs.texas.gov/sites/default/files/coi-policy.pdf"},
    {"org": "Florida Dept of Health", "url": "http://www.floridahealth.gov/about/administrative-functions/general-counsel/_documents/conflict-of-interest-policy.pdf"},
    {"org": "Florida Alt", "url": "https://ahca.myflorida.com/content/download/7653/file/Conflict_of_Interest_Policy.pdf"},
    {"org": "Illinois Dept of Public Health", "url": "https://dph.illinois.gov/content/dam/soi/en/web/idph/files/conflict-of-interest-policy.pdf"},
    {"org": "Illinois Alt", "url": "https://www.illinois.gov/hfs/SiteCollectionDocuments/ConflictofInterestPolicy.pdf"},
    {"org": "Pennsylvania Dept of Health", "url": "https://www.health.pa.gov/topics/Documents/Administrative/Conflict-of-Interest-Policy.pdf"},
    {"org": "Ohio Dept of Health", "url": "https://odh.ohio.gov/wps/wcm/connect/gov/conflict-of-interest-policy.pdf"},
    {"org": "Ohio Alt", "url": "https://medicaid.ohio.gov/static/Resources/COI-Policy.pdf"},
    {"org": "Michigan Dept of Health", "url": "https://www.michigan.gov/mdhhs/-/media/Project/Websites/mdhhs/Folder1/Folder52/COI_Policy.pdf"},
    {"org": "Michigan Alt", "url": "https://www.michigan.gov/documents/mdch/COI_Policy_617918_7.pdf"},
    {"org": "North Carolina DHHS", "url": "https://www.ncdhhs.gov/media/783/download"},
    {"org": "Georgia Dept of Public Health", "url": "https://dph.georgia.gov/document/publication/conflict-interest-policy/download"},
    {"org": "Georgia Alt", "url": "https://dch.georgia.gov/sites/dch.georgia.gov/files/COI-Policy.pdf"},
    {"org": "Virginia Dept of Health", "url": "https://www.vdh.virginia.gov/content/uploads/sites/191/2020/09/VDH-COI-Policy.pdf"},
    {"org": "Virginia Alt", "url": "https://www.dmas.virginia.gov/files/links/220/COI%20Policy.pdf"},
    {"org": "Washington State Dept of Health", "url": "https://www.doh.wa.gov/Portals/1/Documents/1000/COI-Policy.pdf"},
    {"org": "Washington Alt", "url": "https://www.hca.wa.gov/assets/billers-and-providers/conflict-of-interest-policy.pdf"},
    {"org": "Massachusetts Dept of Public Health", "url": "https://www.mass.gov/doc/conflict-of-interest-policy-0/download"},
    {"org": "Massachusetts Alt", "url": "https://www.mass.gov/files/documents/2016/08/tn/conflict-of-interest-guide.pdf"},
    {"org": "New Jersey Dept of Health", "url": "https://www.nj.gov/health/legal/documents/conflict-of-interest.pdf"},
    {"org": "New Jersey Alt", "url": "https://www.state.nj.us/health/forms/conflict-of-interest-policy.pdf"},
    {"org": "Arizona Dept of Health Services", "url": "https://www.azdhs.gov/documents/operations/conflict-of-interest-policy.pdf"},
    {"org": "Arizona Alt", "url": "https://www.azahcccs.gov/Resources/Downloads/COI_Policy.pdf"},
    {"org": "Tennessee Dept of Health", "url": "https://www.tn.gov/content/dam/tn/health/documents/conflict-of-interest-policy.pdf"},
    {"org": "Tennessee Alt", "url": "https://www.tn.gov/tenncare/information-statistics/conflict-of-interest.html"},
    {"org": "Indiana State Dept of Health", "url": "https://www.in.gov/health/files/Conflict-of-Interest-Policy.pdf"},
    {"org": "Indiana Alt", "url": "https://www.in.gov/fssa/files/Conflict_of_Interest_Policy.pdf"},
    {"org": "Missouri Dept of Health", "url": "https://health.mo.gov/about/pdf/conflict-of-interest-policy.pdf"},
    {"org": "Missouri Alt", "url": "https://dss.mo.gov/mhd/general/pdf/coi-policy.pdf"},
    {"org": "Maryland Dept of Health", "url": "https://health.maryland.gov/docs/COI_Policy.pdf"},
    {"org": "Maryland Alt", "url": "https://mmcp.health.maryland.gov/Documents/COI_Policy.pdf"},
    {"org": "Wisconsin Dept of Health Services", "url": "https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf"},
    {"org": "Minnesota Dept of Health", "url": "https://www.health.state.mn.us/about/org/conflict-interest-policy.pdf"},
    {"org": "Minnesota Alt", "url": "https://mn.gov/dhs/partners-and-providers/policies-and-procedures/conflict-of-interest/"},
    {"org": "Colorado Dept of Public Health", "url": "https://cdphe.colorado.gov/sites/cdphe/files/CHED_COI_Policy.pdf"},
    {"org": "Colorado Alt", "url": "https://hcpf.colorado.gov/sites/hcpf/files/Conflict%20of%20Interest%20Policy.pdf"},
    {"org": "Alabama Dept of Public Health", "url": "https://www.alabamapublichealth.gov/legal/assets/coi-policy.pdf"},
    {"org": "South Carolina DHEC", "url": "https://scdhec.gov/sites/default/files/Library/CR-012148.pdf"},
    {"org": "South Carolina Alt", "url": "https://www.scdhhs.gov/sites/default/files/COI_Policy.pdf"},
    {"org": "Louisiana Dept of Health", "url": "https://ldh.la.gov/assets/docs/BayouHealth/ConflictofInterest.pdf"},
    {"org": "Louisiana Alt", "url": "https://ldh.la.gov/assets/medicaid/COI_Policy.pdf"},
    {"org": "Kentucky Cabinet for Health", "url": "https://chfs.ky.gov/agencies/os/oig/Documents/ConflictofInterestPolicy.pdf"},
    {"org": "Kentucky Alt", "url": "https://prd.chfs.ky.gov/Documents/coi_policy.pdf"},
    {"org": "Oregon Health Authority", "url": "https://www.oregon.gov/oha/HPA/HP-PCO/Documents/COI-Policy.pdf"},
    {"org": "Oregon Alt", "url": "https://www.oregon.gov/oha/FOD/PIAU/Documents/COI-Policy.pdf"},
    {"org": "Connecticut Dept of Public Health", "url": "https://portal.ct.gov/-/media/Departments-and-Agencies/DPH/dph/state_health_planning/SHA-SHIP/HCCDocuments/COI_Policy.pdf"},
    {"org": "Connecticut Alt", "url": "https://portal.ct.gov/DSS/Health-And-Home-Care/Conflict-of-Interest-Policy"},
    {"org": "Iowa Dept of Public Health", "url": "https://idph.iowa.gov/Portals/1/userfiles/91/Conflict%20of%20Interest%20Policy.pdf"},
    {"org": "Oklahoma State Dept of Health", "url": "https://oklahoma.gov/content/dam/ok/en/health/health2/documents/conflict-of-interest-policy.pdf"},
    {"org": "Oklahoma Alt", "url": "https://www.ok.gov/health2/documents/COI_Policy.pdf"},
    {"org": "Arkansas Dept of Health", "url": "https://www.healthy.arkansas.gov/images/uploads/pdf/ConflictofInterestPolicy.pdf"},
    {"org": "Arkansas Alt", "url": "https://humanservices.arkansas.gov/wp-content/uploads/COI-Policy.pdf"},
    {"org": "Utah Dept of Health", "url": "https://health.utah.gov/wp-content/uploads/COI-Policy.pdf"},
    {"org": "Kansas Dept of Health", "url": "https://www.kdhe.ks.gov/DocumentCenter/View/124/Conflict-of-Interest-Policy-PDF"},
    {"org": "Nevada Dept of Health", "url": "https://dhhs.nv.gov/uploadedFiles/dhhs.nv.gov/content/Resources/AdminSupport/COI-Policy.pdf"},
    {"org": "Nevada Alt", "url": "https://dpbh.nv.gov/uploadedFiles/dpbhnvgov/content/COI-Policy.pdf"},
    {"org": "New Mexico Dept of Health", "url": "https://www.nmhealth.org/publication/view/policy/6093/"},
    {"org": "New Mexico Alt", "url": "https://www.hsd.state.nm.us/wp-content/uploads/COI-Policy.pdf"},
    {"org": "Mississippi State Dept of Health", "url": "https://msdh.ms.gov/msdhsite/_static/resources/78.pdf"},
    {"org": "West Virginia DHHR", "url": "https://dhhr.wv.gov/bms/Documents/COI_Policy.pdf"},
    {"org": "West Virginia Alt", "url": "https://oeps.wv.gov/coi/Documents/COI_Policy.pdf"},
    {"org": "Hawaii Dept of Health", "url": "https://health.hawaii.gov/opppd/files/2019/08/COI-Policy.pdf"},
    {"org": "Hawaii Alt", "url": "https://medquest.hawaii.gov/content/dam/formsanddocuments/resources/COI-Policy.pdf"},
    {"org": "Maine CDC", "url": "https://www.maine.gov/dhhs/mecdc/public-health-systems/coi-policy.pdf"},
    {"org": "Maine Alt", "url": "https://www.maine.gov/dhhs/sites/maine.gov.dhhs/files/documents/coi-policy.pdf"},
    {"org": "Nebraska DHHS", "url": "http://dhhs.ne.gov/Documents/COI-Policy.pdf"},
    {"org": "Nebraska Alt", "url": "https://dhhs.ne.gov/Medicaid%20Provider%20Bulletins/COI-Policy.pdf"},
    {"org": "Idaho Dept of Health", "url": "https://healthandwelfare.idaho.gov/sites/default/files/2019-11/COI_Policy.pdf"},
    {"org": "Idaho Alt", "url": "https://publichealth.idaho.gov/wp-content/uploads/COI_Policy.pdf"},
    {"org": "Rhode Island Dept of Health", "url": "https://health.ri.gov/publications/policies/ConflictOfInterest.pdf"},
    {"org": "Rhode Island Alt", "url": "https://eohhs.ri.gov/sites/g/files/xkgbur226/files/COI-Policy.pdf"},
    {"org": "Montana DPHHS", "url": "https://dphhs.mt.gov/assets/publichealth/CDEpi/COI_Policy.pdf"},
    {"org": "Montana Alt", "url": "https://dphhs.mt.gov/Portals/85/Documents/COI-Policy.pdf"},
    {"org": "Delaware Health and Social Services", "url": "https://www.dhss.delaware.gov/dhss/admin/files/coi_policy.pdf"},
    {"org": "Delaware Alt", "url": "https://dhss.delaware.gov/dph/files/coipolicy.pdf"},
    {"org": "North Dakota Dept of Health", "url": "https://www.ndhealth.gov/Files/COI_Policy.pdf"},
    {"org": "North Dakota Alt", "url": "https://www.hhs.nd.gov/sites/www/files/documents/COI-Policy.pdf"},
    {"org": "South Dakota Dept of Health", "url": "https://doh.sd.gov/documents/COI-Policy.pdf"},
    {"org": "South Dakota Alt", "url": "https://dss.sd.gov/docs/medicaid/providers/COI-Policy.pdf"},
    {"org": "Vermont Dept of Health", "url": "https://www.healthvermont.gov/sites/default/files/documents/pdf/COI_Policy.pdf"},
    {"org": "Vermont Alt", "url": "https://dvha.vermont.gov/sites/dvha/files/documents/COI-Policy.pdf"},
    {"org": "Alaska DHSS", "url": "https://dhss.alaska.gov/Commissioner/Documents/COI-policy.pdf"},
    {"org": "Alaska Alt", "url": "http://dhss.alaska.gov/dph/Documents/COI-Policy.pdf"},
    {"org": "Wyoming Dept of Health", "url": "https://health.wyo.gov/wp-content/uploads/2020/10/COI-Policy.pdf"},
    {"org": "Wyoming Alt", "url": "https://health.wyo.gov/healthcarefin/medicaid/programs-and-eligibility/conflict-of-interest-policy/"},
    {"org": "DC Health", "url": "https://dchealth.dc.gov/sites/default/files/dc/sites/doh/page_content/attachments/Conflict%20of%20Interest%20Policy.pdf"},
    {"org": "DC Alt", "url": "https://dhcf.dc.gov/sites/default/files/dc/sites/dhcf/page_content/attachments/COI%20Policy.pdf"}
]

def download_policy(policy_data, download_dir="data/raw/policies", timeout=30):
    """Download a single policy document with improved error handling"""
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
        
        # Try different headers
        headers_list = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            },
            {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            }
        ]
        
        # Try with different headers
        for headers in headers_list:
            try:
                response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
                
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
                elif response.status_code == 403:
                    continue  # Try next header
            except:
                continue  # Try next header
        
        # If all headers failed
        logger.warning(f"✗ Failed {org}: Could not download after multiple attempts")
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

def main():
    """Download all federal and state COI policies with enhanced strategies"""
    logger.info(f"Starting enhanced download of {len(enhanced_federal_state_policies)} federal and state COI policy URLs...")
    
    results = []
    successful_downloads = []
    
    # Use thread pool for faster downloads
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_policy = {
            executor.submit(download_policy, policy): policy 
            for policy in enhanced_federal_state_policies
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
    report_file = f"data/output/enhanced_federal_state_{timestamp}.json"
    os.makedirs("data/output", exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_attempted': len(enhanced_federal_state_policies),
            'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
            'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
            'failed': len([r for r in results if not r['success']]),
            'results': results
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print("ENHANCED FEDERAL & STATE COI POLICY DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total URLs attempted: {len(enhanced_federal_state_policies)}")
    print(f"New downloads: {len([r for r in results if r['success'] and r['status'] == 'downloaded'])}")
    print(f"Already existed: {len([r for r in results if r['success'] and r['status'] == 'already_exists'])}")
    print(f"Failed downloads: {len([r for r in results if not r['success']])}")
    print(f"\nReport saved to: {report_file}")
    
    if successful_downloads:
        print(f"\n{'='*60}")
        print("Successfully Downloaded Organizations:")
        print(f"{'='*60}")
        
        # Show federal agencies first
        federal = [org for org in successful_downloads if any(word in org for word in ['HHS', 'CDC', 'FDA', 'NIH', 'CMS', 'HRSA', 'SAMHSA', 'Veterans', 'DOD', 'AHRQ', 'PCORI', 'Indian', 'Public Health', 'National Science', 'EPA', 'NOAA', 'USAID', 'USDA', 'DOE'])]
        if federal:
            print("\nFEDERAL AGENCIES:")
            for org in federal:
                print(f"✓ {org}")
        
        # Show state departments
        state = [org for org in successful_downloads if org not in federal]
        if state:
            print(f"\nSTATE HEALTH DEPARTMENTS ({len(state)} total):")
            for org in state[:25]:  # Show first 25 states
                print(f"✓ {org}")
            if len(state) > 25:
                print(f"... and {len(state) - 25} more states")

if __name__ == "__main__":
    main()