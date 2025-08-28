#!/usr/bin/env python3
"""
Playwright-based COI Policy Scraper for US Healthcare Systems
Finds and downloads COI policies using browser automation
"""

import os
import json
import time
import hashlib
import asyncio
from datetime import datetime
from typing import List, Dict
import logging
from playwright.async_api import async_playwright
import aiohttp
import re
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PlaywrightCOIScraper:
    """Advanced COI policy scraper using Playwright"""
    
    def __init__(self):
        self.found_urls = set()
        self.download_dir = "data/raw/policies"
        self.screenshots_dir = "data/screenshots"
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.screenshots_dir, exist_ok=True)
        self.results = []
        
    async def search_google_for_policies(self, page, query: str, max_results: int = 30):
        """Search Google and extract result URLs"""
        urls = []
        try:
            # Navigate to Google
            await page.goto('https://www.google.com')
            await page.wait_for_timeout(1000)
            
            # Search
            await page.fill('textarea[name="q"]', query)
            await page.press('textarea[name="q"]', 'Enter')
            await page.wait_for_load_state('networkidle')
            await page.wait_for_timeout(2000)
            
            # Extract search results
            links = await page.query_selector_all('a')
            for link in links[:max_results]:
                href = await link.get_attribute('href')
                if href and 'http' in href and 'google.com' not in href:
                    # Clean URL
                    if '/url?q=' in href:
                        href = href.split('/url?q=')[1].split('&')[0]
                    urls.append(href)
                    
            logger.info(f"Found {len(urls)} URLs for query: {query}")
            
        except Exception as e:
            logger.error(f"Error searching Google for '{query}': {e}")
            
        return urls
    
    async def visit_healthcare_site(self, page, org_name: str, base_url: str):
        """Visit a healthcare website and search for COI policies"""
        found_policies = []
        
        try:
            logger.info(f"Visiting {org_name} at {base_url}")
            await page.goto(base_url, wait_until='networkidle', timeout=30000)
            await page.wait_for_timeout(2000)
            
            # Look for search functionality
            search_selectors = [
                'input[type="search"]',
                'input[placeholder*="search" i]',
                'input[name*="search" i]',
                'input[id*="search" i]',
                'input[aria-label*="search" i]'
            ]
            
            search_box = None
            for selector in search_selectors:
                try:
                    search_box = await page.query_selector(selector)
                    if search_box:
                        break
                except:
                    continue
            
            if search_box:
                # Search for COI policy
                await search_box.fill("conflict of interest policy")
                await search_box.press('Enter')
                await page.wait_for_load_state('networkidle')
                await page.wait_for_timeout(3000)
            
            # Find all links that might be COI related
            all_links = await page.query_selector_all('a')
            
            for link in all_links:
                try:
                    href = await link.get_attribute('href')
                    text = await link.text_content()
                    
                    if href and text:
                        text_lower = text.lower()
                        href_lower = href.lower()
                        
                        # Check if link is COI related
                        coi_keywords = ['conflict', 'coi', 'interest', 'disclosure', 'compliance', 'ethics', 'vendor']
                        if any(keyword in text_lower or keyword in href_lower for keyword in coi_keywords):
                            full_url = urljoin(base_url, href)
                            
                            # Check if it's a PDF
                            is_pdf = '.pdf' in href_lower or 'pdf' in text_lower
                            
                            if full_url not in self.found_urls:
                                self.found_urls.add(full_url)
                                policy = {
                                    'organization': org_name,
                                    'url': full_url,
                                    'link_text': text.strip(),
                                    'is_pdf': is_pdf,
                                    'source_url': base_url,
                                    'timestamp': datetime.now().isoformat()
                                }
                                found_policies.append(policy)
                                logger.info(f"Found potential policy: {org_name} - {text.strip()[:50]}")
                except Exception as e:
                    continue
                    
        except Exception as e:
            logger.error(f"Error visiting {org_name}: {e}")
            
        return found_policies
    
    async def download_file(self, session, url: str, org_name: str):
        """Download a file using aiohttp"""
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Determine file extension
                    content_type = response.headers.get('content-type', '').lower()
                    if 'pdf' in content_type or url.lower().endswith('.pdf'):
                        ext = 'pdf'
                    elif 'html' in content_type or 'text' in content_type:
                        ext = 'html'
                    else:
                        ext = 'dat'
                    
                    # Create filename
                    org_clean = re.sub(r'[^\w\s-]', '', org_name).replace(' ', '_')
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                    filename = f"{self.download_dir}/{org_clean}_{url_hash}.{ext}"
                    
                    # Save file
                    with open(filename, 'wb') as f:
                        f.write(content)
                    
                    logger.info(f"✓ Downloaded: {filename}")
                    return {
                        'success': True,
                        'filename': filename,
                        'size': len(content),
                        'type': ext
                    }
                else:
                    return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def run_comprehensive_search(self):
        """Run comprehensive search for COI policies"""
        
        # List of major US healthcare systems
        healthcare_systems = [
            {"name": "Mayo Clinic", "url": "https://www.mayoclinic.org"},
            {"name": "Cleveland Clinic", "url": "https://my.clevelandclinic.org"},
            {"name": "Johns Hopkins", "url": "https://www.hopkinsmedicine.org"},
            {"name": "Mass General Brigham", "url": "https://www.massgeneralbrigham.org"},
            {"name": "Kaiser Permanente", "url": "https://healthy.kaiserpermanente.org"},
            {"name": "UCSF Health", "url": "https://www.ucsfhealth.org"},
            {"name": "UCLA Health", "url": "https://www.uclahealth.org"},
            {"name": "Stanford Health", "url": "https://stanfordhealthcare.org"},
            {"name": "Mount Sinai", "url": "https://www.mountsinai.org"},
            {"name": "NYU Langone", "url": "https://nyulangone.org"},
            {"name": "Penn Medicine", "url": "https://www.pennmedicine.org"},
            {"name": "Duke Health", "url": "https://www.dukehealth.org"},
            {"name": "Vanderbilt Health", "url": "https://www.vanderbilthealth.com"},
            {"name": "Northwestern Medicine", "url": "https://www.nm.org"},
            {"name": "University of Chicago", "url": "https://www.uchicagomedicine.org"},
            {"name": "Michigan Medicine", "url": "https://www.uofmhealth.org"},
            {"name": "Ohio State Medical", "url": "https://wexnermedical.osu.edu"},
            {"name": "UPMC", "url": "https://www.upmc.com"},
            {"name": "Cedars-Sinai", "url": "https://www.cedars-sinai.org"},
            {"name": "Houston Methodist", "url": "https://www.houstonmethodist.org"},
            {"name": "Emory Healthcare", "url": "https://www.emoryhealthcare.org"},
            {"name": "Yale New Haven", "url": "https://www.ynhhs.org"},
            {"name": "Columbia Medical", "url": "https://www.columbiadoctors.org"},
            {"name": "Rush Medical", "url": "https://www.rush.edu"},
            {"name": "Baylor Scott White", "url": "https://www.bswhealth.com"},
            {"name": "Ochsner Health", "url": "https://www.ochsner.org"},
            {"name": "Intermountain", "url": "https://intermountainhealthcare.org"},
            {"name": "Providence", "url": "https://www.providence.org"},
            {"name": "CommonSpirit", "url": "https://www.commonspirit.org"},
            {"name": "Ascension", "url": "https://www.ascension.org"},
            {"name": "HCA Healthcare", "url": "https://hcahealthcare.com"},
            {"name": "Sutter Health", "url": "https://www.sutterhealth.org"},
            {"name": "Banner Health", "url": "https://www.bannerhealth.com"},
            {"name": "Atrium Health", "url": "https://atriumhealth.org"},
            {"name": "Advocate Aurora", "url": "https://www.advocateaurorahealth.org"},
            {"name": "Trinity Health", "url": "https://www.trinity-health.org"},
            {"name": "Spectrum Health", "url": "https://www.spectrumhealth.org"},
            {"name": "Northwell Health", "url": "https://www.northwell.edu"},
            {"name": "ChristianaCare", "url": "https://christianacare.org"},
            {"name": "Geisinger", "url": "https://www.geisinger.org"},
            {"name": "Henry Ford", "url": "https://www.henryford.com"},
            {"name": "Jefferson Health", "url": "https://www.jeffersonhealth.org"},
            {"name": "Memorial Hermann", "url": "https://memorialhermann.org"},
            {"name": "Montefiore", "url": "https://www.montefiore.org"},
            {"name": "NewYork-Presbyterian", "url": "https://www.nyp.org"},
            {"name": "Orlando Health", "url": "https://www.orlandohealth.com"},
            {"name": "Scripps Health", "url": "https://www.scripps.org"},
            {"name": "Sharp Healthcare", "url": "https://www.sharp.com"},
            {"name": "Tampa General", "url": "https://www.tgh.org"},
            {"name": "Wake Forest Baptist", "url": "https://www.wakehealth.edu"},
        ]
        
        # Google search queries
        search_queries = [
            'filetype:pdf "conflict of interest policy" hospital USA -job -careers',
            'filetype:pdf "COI policy" "medical center" -recruitment',
            '"conflict of interest policy" healthcare system PDF',
            '"conflict of interest disclosure" hospital download',
            '"vendor conflict of interest" hospital policy PDF',
            '"physician conflict of interest" policy document',
            '"medical staff conflict of interest" policy PDF',
            '"healthcare compliance" "conflict of interest" policy',
            '"industry relationships" hospital policy PDF',
            '"pharmaceutical relationships" medical center policy',
            'site:*.edu "conflict of interest policy" medical PDF',
            'site:*.org "COI policy" hospital PDF',
            'inurl:policies "conflict of interest" healthcare PDF',
            'inurl:compliance "COI" medical center document',
            'inurl:pdf "conflict of interest" hospital policy',
        ]
        
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # 1. Search Google for policies
            logger.info("=" * 60)
            logger.info("PHASE 1: Google Search for COI Policies")
            logger.info("=" * 60)
            
            for query in search_queries:
                logger.info(f"Searching: {query}")
                urls = await self.search_google_for_policies(page, query)
                
                for url in urls:
                    if url not in self.found_urls:
                        self.found_urls.add(url)
                        
                        # Extract org name from URL
                        domain = urlparse(url).netloc
                        org_name = domain.replace('www.', '').split('.')[0].title()
                        
                        self.results.append({
                            'organization': org_name,
                            'url': url,
                            'is_pdf': '.pdf' in url.lower(),
                            'source': 'google_search',
                            'query': query,
                            'timestamp': datetime.now().isoformat()
                        })
                
                await page.wait_for_timeout(3000)  # Rate limiting
            
            # 2. Visit healthcare websites directly
            logger.info("=" * 60)
            logger.info("PHASE 2: Direct Website Visits")
            logger.info("=" * 60)
            
            for system in healthcare_systems[:20]:  # Start with first 20
                policies = await self.visit_healthcare_site(page, system['name'], system['url'])
                self.results.extend(policies)
                await page.wait_for_timeout(2000)  # Rate limiting
            
            await browser.close()
        
        # 3. Download all found policies
        logger.info("=" * 60)
        logger.info("PHASE 3: Downloading Policy Documents")
        logger.info("=" * 60)
        
        async with aiohttp.ClientSession() as session:
            download_tasks = []
            
            for result in self.results:
                if result.get('is_pdf') or 'pdf' in result['url'].lower():
                    download_tasks.append(
                        self.download_file(session, result['url'], result['organization'])
                    )
            
            # Download in batches
            batch_size = 5
            for i in range(0, len(download_tasks), batch_size):
                batch = download_tasks[i:i+batch_size]
                download_results = await asyncio.gather(*batch, return_exceptions=True)
                
                # Update results with download status
                for j, dl_result in enumerate(download_results):
                    if isinstance(dl_result, dict) and dl_result.get('success'):
                        self.results[i+j]['downloaded'] = True
                        self.results[i+j]['filename'] = dl_result['filename']
                        self.results[i+j]['file_size'] = dl_result['size']
                
                await asyncio.sleep(1)  # Rate limiting between batches
        
        return self.results
    
    def save_results(self):
        """Save search and download results"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"data/output/playwright_results_{timestamp}.json"
        
        # Calculate statistics
        total_found = len(self.results)
        pdfs_found = sum(1 for r in self.results if r.get('is_pdf'))
        downloaded = sum(1 for r in self.results if r.get('downloaded'))
        unique_orgs = len(set(r['organization'] for r in self.results))
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_urls_found': total_found,
                'pdf_documents': pdfs_found,
                'successfully_downloaded': downloaded,
                'unique_organizations': unique_orgs
            },
            'results': self.results
        }
        
        os.makedirs('data/output', exist_ok=True)
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report_file, report['statistics']

async def main():
    """Main execution"""
    scraper = PlaywrightCOIScraper()
    
    logger.info("Starting Playwright COI Policy Scraper...")
    logger.info("This will search Google and visit healthcare websites directly")
    
    results = await scraper.run_comprehensive_search()
    report_file, stats = scraper.save_results()
    
    # Print summary
    print(f"\n{'='*60}")
    print("COI Policy Search Complete")
    print(f"{'='*60}")
    print(f"Total URLs found: {stats['total_urls_found']}")
    print(f"PDF documents: {stats['pdf_documents']}")
    print(f"Successfully downloaded: {stats['successfully_downloaded']}")
    print(f"Unique organizations: {stats['unique_organizations']}")
    print(f"\nResults saved to: {report_file}")
    print(f"Downloaded files in: data/raw/policies/")
    
    # List some successful downloads
    print(f"\n{'='*60}")
    print("Sample Downloaded Policies:")
    print(f"{'='*60}")
    count = 0
    for r in results:
        if r.get('downloaded') and count < 20:
            print(f"✓ {r['organization']}: {r.get('filename', 'N/A')}")
            count += 1

if __name__ == "__main__":
    asyncio.run(main())