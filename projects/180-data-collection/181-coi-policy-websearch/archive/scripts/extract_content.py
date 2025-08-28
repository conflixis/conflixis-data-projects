#!/usr/bin/env python3
"""
Extract content from collected COI policy documents
"""

import os
import json
import logging
import requests
import PyPDF2
import pdfplumber
from bs4 import BeautifulSoup
from typing import Dict, List
from datetime import datetime
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PolicyContentExtractor:
    """Extract and process content from policy documents"""
    
    def __init__(self, raw_dir: str = "data/raw", processed_dir: str = "data/processed"):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
    
    def download_pdf(self, url: str) -> str:
        """Download PDF and return local path"""
        try:
            response = requests.get(url, timeout=30)
            # Generate unique filename
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = f"{self.raw_dir}/policy_{url_hash}.pdf"
            
            with open(filename, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded PDF: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error downloading PDF from {url}: {e}")
            return None
    
    def extract_pdf_content(self, pdf_path: str) -> Dict:
        """Extract text content from PDF"""
        try:
            content = {
                'pages': [],
                'full_text': '',
                'metadata': {}
            }
            
            # Try pdfplumber first (better for tables)
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        content['pages'].append(text)
                        content['full_text'] += text + '\n'
                
                # Extract metadata
                if pdf.metadata:
                    content['metadata'] = {
                        k: str(v) for k, v in pdf.metadata.items()
                    }
            
            return content
        except Exception as e:
            logger.error(f"Error extracting PDF content: {e}")
            return None
    
    def extract_html_content(self, html: str) -> Dict:
        """Extract structured content from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        content = {
            'title': soup.find('title').text if soup.find('title') else '',
            'headers': [],
            'paragraphs': [],
            'lists': [],
            'full_text': soup.get_text()
        }
        
        # Extract headers
        for header in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            content['headers'].append({
                'level': header.name,
                'text': header.text.strip()
            })
        
        # Extract paragraphs
        for p in soup.find_all('p'):
            text = p.text.strip()
            if text and len(text) > 50:  # Filter out short snippets
                content['paragraphs'].append(text)
        
        # Extract lists
        for ul in soup.find_all(['ul', 'ol']):
            items = [li.text.strip() for li in ul.find_all('li')]
            if items:
                content['lists'].append(items)
        
        return content
    
    def identify_key_sections(self, text: str) -> Dict:
        """Identify key COI policy sections"""
        sections = {
            'disclosure_requirements': [],
            'prohibited_activities': [],
            'management_plans': [],
            'definitions': [],
            'penalties': [],
            'review_process': []
        }
        
        # Keywords to identify sections
        keywords = {
            'disclosure_requirements': ['disclosure', 'report', 'declare', 'submit'],
            'prohibited_activities': ['prohibited', 'not allowed', 'forbidden', 'restrict'],
            'management_plans': ['management', 'mitigation', 'resolution', 'plan'],
            'definitions': ['definition', 'means', 'refers to', 'defined as'],
            'penalties': ['penalty', 'violation', 'consequence', 'sanction'],
            'review_process': ['review', 'committee', 'evaluation', 'assessment']
        }
        
        # Split text into sentences
        sentences = text.split('.')
        
        for sentence in sentences:
            sentence_lower = sentence.lower()
            for section, words in keywords.items():
                if any(word in sentence_lower for word in words):
                    sections[section].append(sentence.strip())
        
        return sections
    
    def process_all_policies(self, search_results_path: str = "data/processed/search_results.json"):
        """Process all discovered policies"""
        with open(search_results_path, 'r') as f:
            search_results = json.load(f)
        
        processed_policies = []
        
        for result in search_results:
            policy_data = {
                'source_url': result['url'],
                'title': result['title'],
                'date_collected': result['date_collected'],
                'content': {}
            }
            
            # Process PDFs if available
            if result.get('pdf_links'):
                for pdf_url in result['pdf_links'][:1]:  # Process first PDF
                    pdf_path = self.download_pdf(pdf_url)
                    if pdf_path:
                        pdf_content = self.extract_pdf_content(pdf_path)
                        if pdf_content:
                            policy_data['content'] = pdf_content
                            policy_data['key_sections'] = self.identify_key_sections(
                                pdf_content['full_text']
                            )
            
            # If no PDF, process HTML
            if not policy_data['content'] and result.get('html_content'):
                html_content = self.extract_html_content(result['html_content'])
                policy_data['content'] = html_content
                policy_data['key_sections'] = self.identify_key_sections(
                    html_content['full_text']
                )
            
            if policy_data['content']:
                processed_policies.append(policy_data)
        
        # Save processed policies
        output_path = f"{self.processed_dir}/extracted_policies.json"
        with open(output_path, 'w') as f:
            json.dump(processed_policies, f, indent=2)
        
        logger.info(f"Processed {len(processed_policies)} policies")
        return processed_policies

def main():
    """Main execution function"""
    extractor = PolicyContentExtractor()
    policies = extractor.process_all_policies()
    
    print(f"\n{'='*50}")
    print(f"Content Extraction Complete")
    print(f"{'='*50}")
    print(f"Total policies processed: {len(policies)}")
    print(f"Results saved to: data/processed/extracted_policies.json")

if __name__ == "__main__":
    main()