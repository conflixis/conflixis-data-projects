#!/usr/bin/env python3
"""
Search for COI policies from healthcare organizations
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from googlesearch import search
import yaml
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class COIPolicySearcher:
    """Search and collect COI policies from web sources"""
    
    def __init__(self, config_path: str = "config/sources.yaml"):
        """Initialize with configuration"""
        self.config = self._load_config(config_path)
        self.results = []
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Return default search configuration"""
        return {
            'search_queries': [
                'hospital conflict of interest policy',
                'healthcare COI policy',
                'medical center conflict of interest disclosure',
                'physician COI policy',
                'healthcare compliance conflict of interest'
            ],
            'target_domains': [
                '.edu',  # Academic medical centers
                '.org',  # Non-profit hospitals
                '.gov'   # Government healthcare
            ],
            'max_results_per_query': 10
        }
    
    def search_google(self, query: str, num_results: int = 10) -> List[str]:
        """Search Google for policy URLs"""
        urls = []
        try:
            logger.info(f"Searching Google for: {query}")
            for url in search(query, num_results=num_results):
                # Filter for likely policy pages
                if any(keyword in url.lower() for keyword in ['policy', 'coi', 'conflict', 'compliance']):
                    urls.append(url)
        except Exception as e:
            logger.error(f"Error searching Google: {e}")
        return urls
    
    def extract_policy_metadata(self, url: str) -> Dict:
        """Extract metadata from a policy page"""
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title = soup.find('title').text if soup.find('title') else ''
            
            # Look for PDF links
            pdf_links = []
            for link in soup.find_all('a', href=True):
                if '.pdf' in link['href'].lower():
                    pdf_links.append(link['href'])
            
            return {
                'url': url,
                'title': title.strip(),
                'pdf_links': pdf_links,
                'date_collected': datetime.now().isoformat(),
                'html_content': response.text[:1000]  # Sample of content
            }
        except Exception as e:
            logger.error(f"Error extracting metadata from {url}: {e}")
            return None
    
    def search_all(self) -> List[Dict]:
        """Execute all searches based on configuration"""
        all_urls = set()
        
        # Search using configured queries
        for query in tqdm(self.config['search_queries'], desc="Search queries"):
            urls = self.search_google(
                query, 
                num_results=self.config['max_results_per_query']
            )
            all_urls.update(urls)
        
        # Extract metadata from collected URLs
        logger.info(f"Found {len(all_urls)} unique URLs")
        for url in tqdm(all_urls, desc="Extracting metadata"):
            metadata = self.extract_policy_metadata(url)
            if metadata:
                self.results.append(metadata)
        
        return self.results
    
    def save_results(self, output_path: str = "data/processed/search_results.json"):
        """Save search results to JSON file"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Saved {len(self.results)} results to {output_path}")

def main():
    """Main execution function"""
    searcher = COIPolicySearcher()
    results = searcher.search_all()
    searcher.save_results()
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"COI Policy Search Complete")
    print(f"{'='*50}")
    print(f"Total URLs found: {len(results)}")
    print(f"URLs with PDFs: {sum(1 for r in results if r.get('pdf_links'))}")
    print(f"Results saved to: data/processed/search_results.json")

if __name__ == "__main__":
    main()