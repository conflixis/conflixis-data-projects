#!/usr/bin/env python3
"""
Comprehensive search for US Healthcare COI Policies
Focus: Find as many COI policy URLs as possible from US healthcare systems
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Set
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class USHealthcareCOIFinder:
    """Find COI policies from US healthcare organizations"""
    
    def __init__(self):
        self.results = []
        self.found_urls = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_major_us_healthcare_systems(self) -> List[Dict]:
        """Return list of major US healthcare systems to search"""
        return [
            # Top US Healthcare Systems by size/revenue
            {"name": "HCA Healthcare", "domain": "hcahealthcare.com"},
            {"name": "CommonSpirit Health", "domain": "commonspirit.org"},
            {"name": "Ascension", "domain": "ascension.org"},
            {"name": "Kaiser Permanente", "domain": "kaiserpermanente.org"},
            {"name": "Providence", "domain": "providence.org"},
            {"name": "Advocate Health", "domain": "advocatehealth.org"},
            {"name": "Trinity Health", "domain": "trinity-health.org"},
            {"name": "Cleveland Clinic", "domain": "clevelandclinic.org"},
            {"name": "Mayo Clinic", "domain": "mayoclinic.org"},
            {"name": "Johns Hopkins", "domain": "hopkinsmedicine.org"},
            {"name": "Mass General Brigham", "domain": "massgeneralbrigham.org"},
            {"name": "NYU Langone", "domain": "nyulangone.org"},
            {"name": "Mount Sinai", "domain": "mountsinai.org"},
            {"name": "Northwell Health", "domain": "northwell.edu"},
            {"name": "UPMC", "domain": "upmc.com"},
            {"name": "Penn Medicine", "domain": "pennmedicine.org"},
            {"name": "Cedars-Sinai", "domain": "cedars-sinai.org"},
            {"name": "UCLA Health", "domain": "uclahealth.org"},
            {"name": "Stanford Health", "domain": "stanfordhealthcare.org"},
            {"name": "Duke Health", "domain": "dukehealth.org"},
            {"name": "Vanderbilt Health", "domain": "vanderbilthealth.com"},
            {"name": "Michigan Medicine", "domain": "uofmhealth.org"},
            {"name": "Ohio State Medical", "domain": "wexnermedical.osu.edu"},
            {"name": "Northwestern Medicine", "domain": "nm.org"},
            {"name": "Rush University Medical", "domain": "rush.edu"},
            {"name": "Emory Healthcare", "domain": "emoryhealthcare.org"},
            {"name": "Houston Methodist", "domain": "houstonmethodist.org"},
            {"name": "Baylor Scott & White", "domain": "bswhealth.com"},
            {"name": "Ochsner Health", "domain": "ochsner.org"},
            {"name": "Intermountain Health", "domain": "intermountainhealthcare.org"},
            {"name": "Spectrum Health", "domain": "spectrumhealth.org"},
            {"name": "Dignity Health", "domain": "dignityhealth.org"},
            {"name": "Sutter Health", "domain": "sutterhealth.org"},
            {"name": "Banner Health", "domain": "bannerhealth.com"},
            {"name": "Atrium Health", "domain": "atriumhealth.org"},
            {"name": "Baptist Health", "domain": "baptisthealth.com"},
            {"name": "Beaumont Health", "domain": "beaumont.org"},
            {"name": "ChristianaCare", "domain": "christianacare.org"},
            {"name": "Geisinger", "domain": "geisinger.org"},
            {"name": "Henry Ford Health", "domain": "henryford.com"},
            {"name": "Jefferson Health", "domain": "jeffersonhealth.org"},
            {"name": "Memorial Hermann", "domain": "memorialhermann.org"},
            {"name": "Montefiore", "domain": "montefiore.org"},
            {"name": "NewYork-Presbyterian", "domain": "nyp.org"},
            {"name": "Orlando Health", "domain": "orlandohealth.com"},
            {"name": "Partners Healthcare", "domain": "partners.org"},
            {"name": "Scripps Health", "domain": "scripps.org"},
            {"name": "Sharp HealthCare", "domain": "sharp.com"},
            {"name": "Tampa General", "domain": "tgh.org"},
            {"name": "UT Southwestern", "domain": "utsouthwestern.edu"},
            {"name": "UW Medicine", "domain": "uwmedicine.org"},
            {"name": "Wake Forest Baptist", "domain": "wakehealth.edu"},
            {"name": "Yale New Haven", "domain": "ynhhs.org"},
        ]
    
    def search_direct_urls(self, domain: str) -> Set[str]:
        """Search for COI policy URLs directly on healthcare system websites"""
        coi_urls = set()
        
        # Common COI policy URL patterns
        url_patterns = [
            f"https://{domain}/conflict-of-interest",
            f"https://{domain}/coi-policy",
            f"https://{domain}/compliance/conflict-of-interest",
            f"https://{domain}/about/compliance/coi",
            f"https://{domain}/policies/conflict-of-interest",
            f"https://{domain}/corporate-compliance/coi",
            f"https://{domain}/ethics/conflict-of-interest",
            f"https://{domain}/vendor-relations/coi",
            f"https://{domain}/research/compliance/coi",
            f"https://{domain}/medical-staff/policies/coi",
            f"https://www.{domain}/conflict-of-interest",
            f"https://www.{domain}/compliance/coi",
        ]
        
        for url in url_patterns:
            try:
                response = self.session.head(url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    logger.info(f"Found potential COI page: {url}")
                    coi_urls.add(response.url)
            except:
                pass
        
        return coi_urls
    
    def search_site(self, domain: str, name: str) -> List[Dict]:
        """Search a specific healthcare site for COI policies"""
        found_policies = []
        
        # Try direct URLs first
        direct_urls = self.search_direct_urls(domain)
        
        # Try searching the site
        search_urls = [
            f"https://{domain}/search?q=conflict+of+interest+policy",
            f"https://{domain}/search?q=COI+policy",
            f"https://www.{domain}/search?q=conflict+of+interest",
        ]
        
        for search_url in search_urls:
            try:
                response = self.session.get(search_url, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find all links that might be COI related
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        text = link.text.lower()
                        
                        if any(term in text or term in href.lower() for term in 
                               ['conflict', 'coi', 'interest', 'disclosure', 'compliance']):
                            
                            full_url = urljoin(search_url, href)
                            if full_url not in self.found_urls:
                                self.found_urls.add(full_url)
                                found_policies.append({
                                    'organization': name,
                                    'domain': domain,
                                    'url': full_url,
                                    'link_text': link.text.strip(),
                                    'found_via': 'site_search',
                                    'timestamp': datetime.now().isoformat()
                                })
            except Exception as e:
                logger.debug(f"Error searching {search_url}: {e}")
        
        # Add direct URLs
        for url in direct_urls:
            if url not in self.found_urls:
                self.found_urls.add(url)
                found_policies.append({
                    'organization': name,
                    'domain': domain,
                    'url': url,
                    'link_text': 'Direct COI URL',
                    'found_via': 'direct_access',
                    'timestamp': datetime.now().isoformat()
                })
        
        return found_policies
    
    def google_search_healthcare_coi(self) -> List[Dict]:
        """Use Google Custom Search API to find COI policies"""
        # Note: In production, you'd use the actual Google Search API
        # For now, we'll construct search URLs that could be used with selenium/playwright
        
        search_queries = [
            '"conflict of interest policy" site:*.org healthcare USA',
            '"COI policy" hospital "United States"',
            '"conflict of interest disclosure" medical center',
            '"financial disclosure" physician hospital policy',
            '"vendor relationships" "conflict of interest" healthcare',
            '"industry relationships" policy "medical center"',
            'filetype:pdf "conflict of interest" policy hospital',
            'filetype:pdf COI disclosure healthcare',
            '"sunshine act" compliance policy hospital',
            '"open payments" disclosure policy healthcare',
        ]
        
        google_results = []
        for query in search_queries:
            google_results.append({
                'search_query': query,
                'search_url': f"https://www.google.com/search?q={query.replace(' ', '+')}",
                'timestamp': datetime.now().isoformat()
            })
        
        return google_results
    
    def search_academic_medical_centers(self) -> List[Dict]:
        """Search top US academic medical centers"""
        academic_centers = [
            {"name": "Harvard Medical", "domain": "hms.harvard.edu"},
            {"name": "Johns Hopkins Medicine", "domain": "hopkinsmedicine.org"},
            {"name": "Stanford Medicine", "domain": "med.stanford.edu"},
            {"name": "UCSF Medical", "domain": "ucsf.edu"},
            {"name": "Columbia Medical", "domain": "cuimc.columbia.edu"},
            {"name": "Yale Medicine", "domain": "medicine.yale.edu"},
            {"name": "Penn Medicine", "domain": "pennmedicine.org"},
            {"name": "Washington University", "domain": "medicine.wustl.edu"},
            {"name": "Duke Medicine", "domain": "medschool.duke.edu"},
            {"name": "Michigan Medicine", "domain": "medicine.umich.edu"},
            {"name": "Northwestern Medicine", "domain": "feinberg.northwestern.edu"},
            {"name": "UCLA Medicine", "domain": "medschool.ucla.edu"},
            {"name": "UCSD Medicine", "domain": "medschool.ucsd.edu"},
            {"name": "Vanderbilt Medicine", "domain": "medschool.vanderbilt.edu"},
            {"name": "Emory Medicine", "domain": "med.emory.edu"},
            {"name": "UNC Medicine", "domain": "med.unc.edu"},
            {"name": "Pittsburgh Medicine", "domain": "medschool.pitt.edu"},
            {"name": "Case Western Medicine", "domain": "case.edu/medicine"},
            {"name": "Boston University", "domain": "bumc.bu.edu"},
            {"name": "NYU Grossman", "domain": "med.nyu.edu"},
        ]
        
        results = []
        for center in academic_centers:
            logger.info(f"Searching {center['name']}...")
            policies = self.search_site(center['domain'], center['name'])
            results.extend(policies)
            time.sleep(1)  # Be polite
        
        return results
    
    def search_state_health_departments(self) -> List[Dict]:
        """Search state health department sites"""
        states = [
            {"name": "California Dept of Health", "domain": "cdph.ca.gov"},
            {"name": "Texas Health Services", "domain": "dshs.texas.gov"},
            {"name": "New York Dept of Health", "domain": "health.ny.gov"},
            {"name": "Florida Dept of Health", "domain": "floridahealth.gov"},
            {"name": "Illinois Dept of Health", "domain": "dph.illinois.gov"},
            {"name": "Pennsylvania Dept of Health", "domain": "health.pa.gov"},
            {"name": "Ohio Dept of Health", "domain": "odh.ohio.gov"},
            {"name": "Michigan Dept of Health", "domain": "michigan.gov/mdhhs"},
            {"name": "Massachusetts Dept of Health", "domain": "mass.gov/dph"},
            {"name": "Georgia Dept of Health", "domain": "dph.georgia.gov"},
        ]
        
        results = []
        for state in states:
            logger.info(f"Searching {state['name']}...")
            policies = self.search_site(state['domain'], state['name'])
            results.extend(policies)
            time.sleep(1)
        
        return results
    
    def run_comprehensive_search(self) -> Dict:
        """Run comprehensive search for US healthcare COI policies"""
        logger.info("Starting comprehensive US Healthcare COI Policy search...")
        
        all_results = {
            'search_timestamp': datetime.now().isoformat(),
            'healthcare_systems': [],
            'academic_centers': [],
            'state_departments': [],
            'google_searches': [],
            'summary': {}
        }
        
        # Search major healthcare systems
        logger.info("Searching major US healthcare systems...")
        for system in self.get_major_us_healthcare_systems():
            logger.info(f"Searching {system['name']}...")
            policies = self.search_site(system['domain'], system['name'])
            all_results['healthcare_systems'].extend(policies)
            time.sleep(1)  # Rate limiting
        
        # Search academic medical centers
        logger.info("Searching academic medical centers...")
        all_results['academic_centers'] = self.search_academic_medical_centers()
        
        # Search state health departments
        logger.info("Searching state health departments...")
        all_results['state_departments'] = self.search_state_health_departments()
        
        # Prepare Google searches
        all_results['google_searches'] = self.google_search_healthcare_coi()
        
        # Summary statistics
        all_results['summary'] = {
            'total_urls_found': len(self.found_urls),
            'healthcare_systems_searched': len(self.get_major_us_healthcare_systems()),
            'healthcare_policies_found': len(all_results['healthcare_systems']),
            'academic_policies_found': len(all_results['academic_centers']),
            'state_policies_found': len(all_results['state_departments']),
            'unique_organizations': len(set(
                p['organization'] for p in 
                all_results['healthcare_systems'] + 
                all_results['academic_centers'] + 
                all_results['state_departments']
            ))
        }
        
        return all_results
    
    def save_results(self, results: Dict, filename: str = None):
        """Save search results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data/output/us_healthcare_coi_policies_{timestamp}.json"
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {filename}")
        return filename

def main():
    """Main execution"""
    finder = USHealthcareCOIFinder()
    results = finder.run_comprehensive_search()
    output_file = finder.save_results(results)
    
    # Print summary
    print(f"\n{'='*60}")
    print("US Healthcare COI Policy Search Complete")
    print(f"{'='*60}")
    print(f"Total unique URLs found: {results['summary']['total_urls_found']}")
    print(f"Healthcare systems searched: {results['summary']['healthcare_systems_searched']}")
    print(f"Healthcare policies found: {results['summary']['healthcare_policies_found']}")
    print(f"Academic policies found: {results['summary']['academic_policies_found']}")
    print(f"State policies found: {results['summary']['state_policies_found']}")
    print(f"Unique organizations: {results['summary']['unique_organizations']}")
    print(f"\nResults saved to: {output_file}")
    print(f"\nGoogle search queries prepared: {len(results['google_searches'])}")
    print("Note: Run these Google searches manually or with selenium/playwright for more results")

if __name__ == "__main__":
    main()