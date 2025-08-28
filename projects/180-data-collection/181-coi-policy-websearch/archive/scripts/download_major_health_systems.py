#!/usr/bin/env python3
"""
Download COI policies from major US health systems
Focus on the largest healthcare networks in the United States
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

# Top US health systems by revenue/size
major_health_systems = [
    # Top 10 largest US health systems
    {"org": "HCA Healthcare", "url": "https://hcahealthcare.com/ethics/assets/docs/conflict-of-interest-policy.pdf"},
    {"org": "CommonSpirit Health", "url": "https://www.commonspirit.org/-/media/project/commonspirit/commonspirit/pdf/conflict-of-interest-policy.pdf"},
    {"org": "Ascension", "url": "https://www.ascension.org/-/media/ascension/compliance-and-ethics/conflict-of-interest-policy.pdf"},
    {"org": "Providence", "url": "https://www.providence.org/-/media/project/psjh/shared/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Trinity Health", "url": "https://www.trinity-health.org/assets/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Kaiser Permanente", "url": "https://healthy.kaiserpermanente.org/static/health/pdfs/conflict-of-interest-policy.pdf"},
    {"org": "Advocate Aurora Health", "url": "https://www.advocateaurorahealth.org/-/media/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "AdventHealth", "url": "https://www.adventhealth.com/-/media/adventhealth/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "UPMC", "url": "https://www.upmc.com/-/media/upmc/about/compliance/conflict-of-interest-policy.pdf"},
    {"org": "NYU Langone Health", "url": "https://nyulangone.org/files/conflict-of-interest-policy.pdf"},
    
    # Major regional health systems
    {"org": "Sutter Health", "url": "https://www.sutterhealth.org/pdf/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Banner Health", "url": "https://www.bannerhealth.com/-/media/files/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Baylor Scott & White Health", "url": "https://www.bswhealth.com/SiteCollectionDocuments/about/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Spectrum Health", "url": "https://www.spectrumhealth.org/-/media/spectrumhealth/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Ochsner Health", "url": "https://www.ochsner.org/-/media/files/ochsner/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Baptist Health South Florida", "url": "https://baptisthealth.net/baptist-health-news/wp-content/uploads/conflict-of-interest-policy.pdf"},
    {"org": "Dignity Health", "url": "https://www.dignityhealth.org/-/media/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "ChristianaCare", "url": "https://christianacare.org/documents/conflict-of-interest-policy.pdf"},
    {"org": "Northwell Health", "url": "https://www.northwell.edu/sites/northwell.edu/files/conflict-of-interest-policy.pdf"},
    {"org": "Beaumont Health", "url": "https://www.beaumont.org/docs/default-source/compliance/conflict-of-interest-policy.pdf"},
    
    # Academic health systems
    {"org": "NewYork-Presbyterian", "url": "https://www.nyp.org/documents/conflict-of-interest-policy.pdf"},
    {"org": "Cleveland Clinic", "url": "https://my.clevelandclinic.org/-/media/files/org/locations/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Johns Hopkins Medicine", "url": "https://www.hopkinsmedicine.org/som/faculty/policies/facultypolicies/conflict-of-interest-policy.pdf"},
    {"org": "UC Health", "url": "https://www.uchealth.org/-/media/files/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Rush University Medical Center", "url": "https://www.rush.edu/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Cedars-Sinai", "url": "https://www.cedars-sinai.org/content/dam/cedars-sinai/corporate/conflict-of-interest-policy.pdf"},
    {"org": "Stanford Health Care", "url": "https://stanfordhealthcare.org/content/dam/SHC/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Yale New Haven Health", "url": "https://www.ynhh.org/-/media/files/ynhhs/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Mass General Brigham", "url": "https://www.massgeneralbrigham.org/notices/conflict-of-interest-policy.pdf"},
    {"org": "University of Miami Health", "url": "https://umiamihealth.org/~/media/compliance/conflict-of-interest-policy.pdf"},
    
    # Large integrated health systems
    {"org": "Intermountain Health", "url": "https://intermountainhealthcare.org/ckr-ext/Dcmnt?ncid=521025464"},
    {"org": "Henry Ford Health", "url": "https://www.henryford.com/-/media/files/henry-ford/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Hackensack Meridian Health", "url": "https://www.hackensackmeridianhealth.org/-/media/project/hmh/hmh/public/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Jefferson Health", "url": "https://www.jeffersonhealth.org/content/dam/jeff-health/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Penn State Health", "url": "https://www.pennstatehealth.org/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "OSF HealthCare", "url": "https://www.osfhealthcare.org/media/filer_public/conflict-of-interest-policy.pdf"},
    {"org": "Froedtert Health", "url": "https://www.froedtert.com/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "SSM Health", "url": "https://www.ssmhealth.com/docs/default-source/compliance/conflict-of-interest-policy.pdf"},
    {"org": "SCL Health", "url": "https://www.sclhealth.org/-/media/files/compliance/conflict-of-interest-policy.pdf"},
    {"org": "WellSpan Health", "url": "https://www.wellspan.org/media/conflict-of-interest-policy.pdf"},
    
    # Major children's hospital systems
    {"org": "Children's Hospital of Philadelphia", "url": "https://www.chop.edu/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Texas Children's Hospital", "url": "https://www.texaschildrens.org/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Cincinnati Children's", "url": "https://www.cincinnatichildrens.org/-/media/cincinnati-childrens/home/research/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Children's Healthcare of Atlanta", "url": "https://www.choa.org/-/media/files/choa/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Seattle Children's", "url": "https://www.seattlechildrens.org/globalassets/documents/healthcare-professionals/conflict-of-interest-policy.pdf"},
    {"org": "Nationwide Children's Hospital", "url": "https://www.nationwidechildrens.org/-/media/nch/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Children's Hospital Los Angeles", "url": "https://www.chla.org/sites/default/files/atoms/files/conflict-of-interest-policy.pdf"},
    {"org": "Children's Hospital Colorado", "url": "https://www.childrenscolorado.org/globalassets/documents/healthcare-professionals/conflict-of-interest-policy.pdf"},
    
    # Other major systems
    {"org": "Geisinger", "url": "https://www.geisinger.org/-/media/geisinger/pdfs/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Sanford Health", "url": "https://www.sanfordhealth.org/-/media/org/files/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Carolinas HealthCare System", "url": "https://atriumhealth.org/-/media/atrium/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Orlando Health", "url": "https://www.orlandohealth.com/-/media/files/orlandohealth/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Tampa General Hospital", "url": "https://www.tgh.org/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Scripps Health", "url": "https://www.scripps.org/sparkle-assets/documents/conflict-of-interest-policy.pdf"},
    {"org": "Sharp HealthCare", "url": "https://www.sharp.com/about/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Bon Secours Mercy Health", "url": "https://www.bsmhealth.org/-/media/mercy-health/documents/compliance/conflict-of-interest-policy.pdf"},
    {"org": "UnityPoint Health", "url": "https://www.unitypoint.org/filesimages/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Avera Health", "url": "https://www.avera.org/app/files/public/conflict-of-interest-policy.pdf"},
    
    # VA and government-affiliated
    {"org": "Veterans Health Administration", "url": "https://www.va.gov/ethics/docs/conflict-of-interest-policy.pdf"},
    
    # Large west coast systems
    {"org": "Swedish Health Services", "url": "https://www.swedish.org/about/compliance/conflict-of-interest-policy.pdf"},
    {"org": "PeaceHealth", "url": "https://www.peacehealth.org/sites/default/files/conflict-of-interest-policy.pdf"},
    {"org": "Legacy Health", "url": "https://www.legacyhealth.org/-/media/files/pdf/compliance/conflict-of-interest-policy.pdf"},
    {"org": "Hoag Health", "url": "https://www.hoag.org/documents/conflict-of-interest-policy.pdf"},
    {"org": "MemorialCare", "url": "https://www.memorialcare.org/sites/default/files/media/conflict-of-interest-policy.pdf"},
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
    """Download all major health system COI policies"""
    logger.info(f"Starting download of {len(major_health_systems)} major health system COI policies...")
    
    results = []
    successful_downloads = []
    
    # Use thread pool for faster downloads
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_policy = {
            executor.submit(download_policy, policy): policy 
            for policy in major_health_systems
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
    report_file = f"data/output/major_health_systems_download_{timestamp}.json"
    os.makedirs("data/output", exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_attempted': len(major_health_systems),
            'successful': len([r for r in results if r['success'] and r['status'] == 'downloaded']),
            'already_exist': len([r for r in results if r['success'] and r['status'] == 'already_exists']),
            'failed': len([r for r in results if not r['success']]),
            'results': results
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print("MAJOR HEALTH SYSTEMS COI POLICY DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total policies attempted: {len(major_health_systems)}")
    print(f"New downloads: {len([r for r in results if r['success'] and r['status'] == 'downloaded'])}")
    print(f"Already existed: {len([r for r in results if r['success'] and r['status'] == 'already_exists'])}")
    print(f"Failed downloads: {len([r for r in results if not r['success']])}")
    print(f"\nReport saved to: {report_file}")
    
    if successful_downloads:
        print(f"\n{'='*60}")
        print("Newly Downloaded Organizations:")
        print(f"{'='*60}")
        
        for org in successful_downloads[:30]:  # Show first 30
            print(f"✓ {org}")
        
        if len(successful_downloads) > 30:
            print(f"... and {len(successful_downloads) - 30} more")

if __name__ == "__main__":
    main()