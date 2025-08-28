#!/usr/bin/env python3
"""
Google Search for US Healthcare COI Policies
Uses web scraping to find COI policy documents
"""

import os
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote, urlparse
import hashlib
from datetime import datetime
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleCOISearcher:
    """Search Google for US Healthcare COI policies"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.found_urls = set()
        self.download_dir = "data/raw/policies"
        os.makedirs(self.download_dir, exist_ok=True)
    
    def google_search(self, query, num_results=10):
        """Perform Google search and return URLs"""
        urls = []
        
        # Google search URL
        search_url = f"https://www.google.com/search?q={quote(query)}&num={num_results}"
        
        try:
            response = self.session.get(search_url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links in search results
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if href.startswith('/url?q='):
                    # Extract actual URL from Google redirect
                    url = href.split('/url?q=')[1].split('&')[0]
                    if url.startswith('http'):
                        urls.append(url)
            
            time.sleep(2)  # Be polite to Google
            
        except Exception as e:
            logger.error(f"Error searching Google: {e}")
        
        return urls
    
    def search_healthcare_coi_policies(self):
        """Search for COI policies from major US healthcare systems"""
        
        # Targeted search queries for US healthcare COI policies
        search_queries = [
            'filetype:pdf "conflict of interest policy" hospital site:.org',
            'filetype:pdf "COI policy" "healthcare" site:.edu',
            'filetype:pdf "conflict of interest" "medical center" USA',
            '"conflict of interest policy" "Cleveland Clinic"',
            '"conflict of interest policy" "Johns Hopkins Medicine"',
            '"conflict of interest policy" "Mayo Clinic"',
            '"conflict of interest policy" "Mount Sinai Health System"',
            '"conflict of interest policy" "NewYork-Presbyterian"',
            '"conflict of interest policy" "UCLA Medical Center"',
            '"conflict of interest policy" "Stanford Healthcare"',
            '"conflict of interest policy" "Mass General Hospital"',
            '"conflict of interest policy" "Brigham and Women\'s"',
            '"conflict of interest policy" "UCSF Medical Center"',
            '"conflict of interest policy" "Duke University Hospital"',
            '"conflict of interest policy" "Vanderbilt Medical Center"',
            '"conflict of interest policy" "Northwestern Memorial Hospital"',
            '"conflict of interest policy" "University of Michigan Medicine"',
            '"conflict of interest policy" "Yale New Haven Hospital"',
            '"conflict of interest policy" "Columbia University Medical Center"',
            '"conflict of interest policy" "University of Pennsylvania Health"',
            '"conflict of interest policy" "Cedars-Sinai Medical Center"',
            '"conflict of interest policy" "Houston Methodist"',
            '"conflict of interest policy" "Cleveland Clinic Florida"',
            '"conflict of interest policy" "Emory Healthcare"',
            '"conflict of interest policy" "University of Pittsburgh Medical"',
            '"conflict of interest policy" "Rush University Medical Center"',
            '"conflict of interest policy" "University of Chicago Medicine"',
            '"conflict of interest policy" "Washington University Medical Center"',
            '"conflict of interest policy" "Oregon Health Science University"',
            '"conflict of interest policy" "University of Colorado Hospital"',
            'site:hopkinsmedicine.org "conflict of interest" filetype:pdf',
            'site:mayoclinic.org "conflict of interest policy"',
            'site:clevelandclinic.org "COI policy"',
            'site:mountsinai.org "conflict of interest"',
            'site:nyp.org "conflict of interest policy"',
            'site:uclahealth.org "COI disclosure"',
            'site:stanfordhealthcare.org "conflict of interest"',
            'site:massgeneral.org "conflict of interest policy"',
            'site:brighamandwomens.org "COI policy"',
            'site:ucsfhealth.org "conflict of interest"',
            'site:dukehealth.org "conflict of interest policy"',
            'site:vanderbilthealth.com "COI policy"',
            'site:nm.org "conflict of interest"',
            'site:med.umich.edu "conflict of interest policy"',
            'site:ynhh.org "COI policy"',
            'site:columbiadoctors.org "conflict of interest"',
            'site:pennmedicine.org "conflict of interest policy"',
            'site:cedars-sinai.org "COI disclosure"',
            'site:houstonmethodist.org "conflict of interest"',
            'site:my.clevelandclinic.org "COI policy"',
        ]
        
        all_results = []
        
        for query in search_queries:
            logger.info(f"Searching: {query}")
            urls = self.google_search(query, num_results=20)
            
            for url in urls:
                if url not in self.found_urls:
                    self.found_urls.add(url)
                    
                    # Extract organization name from query or URL
                    org_name = self._extract_org_name(query, url)
                    
                    result = {
                        'url': url,
                        'organization': org_name,
                        'search_query': query,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Check if it's a PDF
                    if '.pdf' in url.lower():
                        result['type'] = 'pdf'
                    else:
                        result['type'] = 'webpage'
                    
                    all_results.append(result)
                    logger.info(f"Found: {org_name} - {url}")
        
        return all_results
    
    def _extract_org_name(self, query, url):
        """Extract organization name from query or URL"""
        # Try to extract from query first
        if '"' in query:
            parts = query.split('"')
            for part in parts:
                if 'hospital' in part.lower() or 'medical' in part.lower() or 'clinic' in part.lower():
                    return part.strip()
        
        # Extract from domain
        domain = urlparse(url).netloc
        domain_parts = domain.replace('www.', '').split('.')
        if domain_parts:
            return domain_parts[0].replace('-', ' ').title()
        
        return "Unknown Organization"
    
    def download_found_policies(self, results):
        """Download all found policy documents"""
        downloaded = []
        
        for result in results:
            if result['type'] == 'pdf' or 'pdf' in result['url'].lower():
                try:
                    response = self.session.get(result['url'], timeout=30)
                    if response.status_code == 200:
                        # Create filename
                        org_clean = result['organization'].replace(' ', '_').replace('/', '_')
                        url_hash = hashlib.md5(result['url'].encode()).hexdigest()[:8]
                        filename = f"{self.download_dir}/{org_clean}_{url_hash}.pdf"
                        
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        
                        result['downloaded'] = True
                        result['filename'] = filename
                        result['size'] = len(response.content)
                        downloaded.append(result)
                        logger.info(f"Downloaded: {filename}")
                    else:
                        result['downloaded'] = False
                        result['error'] = f"HTTP {response.status_code}"
                except Exception as e:
                    result['downloaded'] = False
                    result['error'] = str(e)
                    logger.error(f"Failed to download {result['url']}: {e}")
                
                time.sleep(1)  # Rate limiting
        
        return downloaded

def main():
    """Main execution"""
    searcher = GoogleCOISearcher()
    
    logger.info("Starting Google search for US Healthcare COI policies...")
    results = searcher.search_healthcare_coi_policies()
    
    logger.info(f"Found {len(results)} potential COI policy URLs")
    
    # Download PDFs
    logger.info("Downloading PDF documents...")
    downloaded = searcher.download_found_policies(results)
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"data/output/google_search_results_{timestamp}.json"
    
    with open(report_file, 'w') as f:
        json.dump({
            'search_timestamp': datetime.now().isoformat(),
            'total_urls_found': len(results),
            'pdfs_downloaded': len(downloaded),
            'results': results
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print("Google COI Policy Search Complete")
    print(f"{'='*60}")
    print(f"Total URLs found: {len(results)}")
    print(f"PDF documents found: {sum(1 for r in results if r['type'] == 'pdf')}")
    print(f"Successfully downloaded: {len(downloaded)}")
    print(f"\nResults saved to: {report_file}")
    
    # List downloaded files
    if downloaded:
        print(f"\n{'='*60}")
        print("Downloaded Policy Documents:")
        print(f"{'='*60}")
        for doc in downloaded:
            if doc.get('downloaded'):
                print(f"✓ {doc['organization']}: {doc.get('filename', 'N/A')}")

if __name__ == "__main__":
    main()