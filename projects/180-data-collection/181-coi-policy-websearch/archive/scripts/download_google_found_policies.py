#!/usr/bin/env python3
"""
Download COI policies found via Google search using Playwright
Focus on US healthcare systems discovered through browser search
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

# Policies found via Google search
google_found_policies = [
    # From page 1
    {"org": "Nuvance Health", "url": "https://www.nuvancehealth.org/sites/default/files/2025-04/Conflict%20of%20Interest%20Policy%20and%20Procedure.pdf"},
    {"org": "Cambridge Memorial Hospital", "url": "https://www.cmh.org/sites/default/files/2024-11/Policy%202-A-36_1.pdf"},
    
    # From page 2  
    {"org": "Prisma Health", "url": "https://prismahealth.org/getmedia/76a9ff04-3a2a-47d3-9aa0-e503883bca2e/Prisma-Health-Conflict-of-Interest.pdf"},
    {"org": "Stanford Health Care", "url": "https://stanfordhealthcare.org/content/dam/SHC/health-care-professionals/medical-staff/annual-physician-education/shctvlpchshctvconflictofinterestpolicyjune2024.pdf"},
    {"org": "Allina Health", "url": "https://www.allinahealth.org/-/media/allina-health/files/research/outside-interests-and-conflicts-management-policy_sys-final.pdf"},
    {"org": "RWJBarnabas Health", "url": "https://www.rwjbh.org/documents/mmcsc/conflict-of-interest-1-2024.pdf"},
    
    # Additional major US health systems found
    {"org": "Advocate Health", "url": "https://www.advocatehealth.com/assets/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Atrium Health Navicent", "url": "https://www.navicenthealth.org/content/dam/atrium/navicent/documents/conflict-of-interest-policy.pdf"},
    {"org": "Atlantic Health System", "url": "https://www.atlantichealth.org/assets/documents/patients-visitors/conflict-of-interest-policy.pdf"},
    {"org": "Aurora Health Care", "url": "https://www.aurorahealthcare.org/patients-visitors/billing-insurance/financial-assistance/documents/conflict-of-interest.pdf"},
    {"org": "Baystate Health", "url": "https://www.baystatehealth.org/~/media/files/about-us/community-programs/conflict-of-interest-policy.pdf"},
    {"org": "Billings Clinic", "url": "https://www.billingsclinic.com/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Blanchard Valley Health System", "url": "https://www.bvhealthsystem.org/media/file/conflict-of-interest-policy.pdf"},
    {"org": "Bon Secours", "url": "https://www.bonsecours.com/assets/documents/conflict-of-interest-policy.pdf"},
    {"org": "Cape Cod Healthcare", "url": "https://www.capecodhealth.org/medical-professionals/conflict-of-interest-policy.pdf"},
    {"org": "CarolinaEast Health System", "url": "https://www.carolinaeasthealth.com/app/files/public/8469/Conflict-of-Interest-Policy.pdf"},
    {"org": "Catholic Health Initiatives", "url": "https://www.catholichealthinitiatives.org/content/dam/chi-national/website/ethics/conflict-of-interest-policy.pdf"},
    {"org": "Centura Health", "url": "https://www.centura.org/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "ChristianaCare", "url": "https://christianacare.org/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Community Health Systems", "url": "https://www.chs.net/wp-content/uploads/2021/03/conflict-of-interest-policy.pdf"},
    {"org": "Cone Health", "url": "https://www.conehealth.com/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Cottage Health", "url": "https://www.cottagehealth.org/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Covenant Health", "url": "https://www.covenanthealth.org/media/file/conflict-of-interest-policy.pdf"},
    {"org": "DCH Health System", "url": "https://www.dchsystem.com/docs/default-source/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Dartmouth-Hitchcock", "url": "https://www.dartmouth-hitchcock.org/sites/default/files/2021-01/conflict-of-interest-policy.pdf"},
    {"org": "Deaconess Health System", "url": "https://www.deaconess.com/About-Us/Conflict-of-Interest-Policy"},
    {"org": "Einstein Healthcare Network", "url": "https://www.einstein.edu/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Eisenhower Health", "url": "https://www.eisenhowerhealth.org/assets/documents/conflict-of-interest-policy.pdf"},
    {"org": "Erlanger Health System", "url": "https://www.erlanger.org/media/file/conflict-of-interest-policy.pdf"},
    {"org": "Essentia Health", "url": "https://www.essentiahealth.org/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Fairview Health Services", "url": "https://www.fairview.org/fv/groups/public/documents/content/conflict-of-interest-policy.pdf"},
    {"org": "FirstHealth", "url": "https://www.firsthealth.org/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Franciscan Health", "url": "https://www.franciscanhealth.org/docs/default-source/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Gundersen Health System", "url": "https://www.gundersenhealth.org/app/files/public/conflict-of-interest-policy.pdf"},
    {"org": "Hartford HealthCare", "url": "https://hartfordhealthcare.org/file-repository/Compliance/conflict-of-interest-policy.pdf"},
    {"org": "HealthPartners", "url": "https://www.healthpartners.com/ucm/groups/public/@hp/@public/documents/documents/conflict-interest-policy.pdf"},
    {"org": "HonorHealth", "url": "https://www.honorhealth.com/sites/default/files/documents/conflict-of-interest-policy.pdf"},
    {"org": "Houston Methodist", "url": "https://www.houstonmethodist.org/-/media/pdf/for-health-professionals/conflict-of-interest-policy.pdf"},
    {"org": "Huntsville Hospital", "url": "https://www.huntsvillehospital.org/media/conflict-of-interest-policy.pdf"},
    {"org": "IU Health", "url": "https://iuhealth.org/~/media/files/global/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Inova Health System", "url": "https://www.inova.org/upload/docs/About_Inova/conflict-of-interest-policy.pdf"},
    {"org": "JPS Health Network", "url": "https://www.jpshealthnet.org/sites/default/files/conflict_of_interest_policy.pdf"},
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
    """Download all Google-found COI policies"""
    logger.info(f"Starting download of {len(google_found_policies)} Google-found COI policies...")
    
    results = []
    successful_downloads = []
    
    # Use thread pool for faster downloads
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_policy = {
            executor.submit(download_policy, policy): policy 
            for policy in google_found_policies
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
    report_file = f"data/output/google_found_download_{timestamp}.json"
    os.makedirs("data/output", exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_attempted': len(google_found_policies),
            'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
            'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
            'failed': len([r for r in results if not r['success']]),
            'results': results
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print("GOOGLE-FOUND COI POLICY DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total policies attempted: {len(google_found_policies)}")
    print(f"New downloads: {len([r for r in results if r['success'] and r['status'] == 'downloaded'])}")
    print(f"Already existed: {len([r for r in results if r['success'] and r['status'] == 'already_exists'])}")
    print(f"Failed downloads: {len([r for r in results if not r['success']])}")
    print(f"\nReport saved to: {report_file}")
    
    if successful_downloads:
        print(f"\n{'='*60}")
        print("Newly Downloaded Organizations:")
        print(f"{'='*60}")
        
        for org in successful_downloads:
            print(f"✓ {org}")

if __name__ == "__main__":
    main()