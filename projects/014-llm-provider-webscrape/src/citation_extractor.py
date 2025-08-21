"""
Citation Extraction and Validation Module
DA-173: Provider Profile Web Enrichment POC
"""

import re
import json
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
from datetime import datetime

class CitationExtractor:
    """
    Extracts, validates, and processes citations from LLM web search responses.
    Maps data points to their sources and assesses source credibility.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the citation extractor.
        
        Args:
            config: Configuration dictionary from config.yaml
        """
        self.config = config
        self.credibility_weights = config.get("source_credibility", {})
        self.trusted_domains = config.get("trusted_domains", [])
        self.excluded_domains = config.get("excluded_domains", [])
        
    def extract_citations(self, raw_response: str) -> List[Dict[str, Any]]:
        """
        Extract all citations from the LLM response.
        
        Args:
            raw_response: Raw text response from OpenAI web search
            
        Returns:
            List of citation dictionaries with URLs, types, and confidence
        """
        citations = []
        
        # Pattern to find URLs with context
        # Matches [Source: URL] format and standalone URLs
        source_pattern = r'\[Source:\s*(https?://[^\]]+)\]'
        url_pattern = r'https?://[^\s\)\]\}"\'>]+'
        
        # Extract [Source: URL] citations
        source_matches = re.finditer(source_pattern, raw_response, re.IGNORECASE)
        for match in source_matches:
            url = match.group(1).strip()
            context = self._extract_context(raw_response, match.start(), match.end())
            citation = self._create_citation(url, context, "inline_citation")
            if citation:
                citations.append(citation)
        
        # Extract standalone URLs (not already captured)
        captured_urls = {c["url"] for c in citations}
        url_matches = re.finditer(url_pattern, raw_response)
        for match in url_matches:
            url = match.group(0).strip()
            if url not in captured_urls:
                context = self._extract_context(raw_response, match.start(), match.end())
                citation = self._create_citation(url, context, "extracted_url")
                if citation:
                    citations.append(citation)
                    captured_urls.add(url)
        
        # Extract from JSON if present
        json_citations = self._extract_from_json(raw_response)
        for jc in json_citations:
            if jc["url"] not in captured_urls:
                citations.append(jc)
                captured_urls.add(jc["url"])
        
        # Deduplicate and enhance citations
        citations = self._deduplicate_citations(citations)
        citations = self._enhance_citations(citations)
        
        # Sort by confidence
        citations.sort(key=lambda x: x.get("confidence", 0), reverse=True)
        
        return citations
    
    def _extract_context(self, text: str, start: int, end: int, context_size: int = 200) -> str:
        """
        Extract surrounding context for a citation.
        
        Args:
            text: Full text
            start: Start position of citation
            end: End position of citation
            context_size: Characters to include before/after
            
        Returns:
            Context string
        """
        context_start = max(0, start - context_size)
        context_end = min(len(text), end + context_size)
        
        # Find sentence boundaries
        before = text[context_start:start]
        after = text[end:context_end]
        
        # Try to start at sentence beginning
        sentence_start = before.rfind('. ')
        if sentence_start != -1:
            before = before[sentence_start + 2:]
        
        # Try to end at sentence end
        sentence_end = after.find('. ')
        if sentence_end != -1:
            after = after[:sentence_end + 1]
        
        return (before + text[start:end] + after).strip()
    
    def _create_citation(self, url: str, context: str, citation_type: str) -> Optional[Dict]:
        """
        Create a citation dictionary from URL and context.
        
        Args:
            url: The URL to cite
            context: Surrounding context
            citation_type: Type of citation extraction
            
        Returns:
            Citation dictionary or None if invalid
        """
        # Validate URL
        if not self._validate_url(url):
            return None
        
        # Parse URL
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check excluded domains
        if any(excluded in domain for excluded in self.excluded_domains):
            return None
        
        # Determine source type
        source_type = self._classify_source(domain, url, context)
        
        # Extract data points from context
        data_points = self._extract_data_points(context)
        
        # Calculate confidence
        confidence = self._calculate_citation_confidence(domain, source_type, context)
        
        citation = {
            "url": url,
            "domain": domain,
            "source_type": source_type,
            "citation_type": citation_type,
            "context": context[:500],  # Limit context length
            "data_points": data_points,
            "confidence": confidence,
            "extracted_at": datetime.now().isoformat(),
            "is_trusted": self._is_trusted_domain(domain)
        }
        
        return citation
    
    def _extract_from_json(self, text: str) -> List[Dict]:
        """
        Extract citations from JSON structures in the response.
        
        Args:
            text: Response text that may contain JSON
            
        Returns:
            List of citations found in JSON
        """
        citations = []
        
        # Try to find JSON blocks
        json_pattern = r'\{[^{}]*"(?:source|url|citation|reference)"[^{}]*\}'
        matches = re.finditer(json_pattern, text, re.DOTALL)
        
        for match in matches:
            try:
                json_str = match.group(0)
                # Clean up common issues
                json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                json_str = re.sub(r'\\n', ' ', json_str)    # Replace newlines
                
                data = json.loads(json_str)
                
                # Look for URL fields
                url = None
                for key in ["source", "url", "citation", "reference", "link"]:
                    if key in data and isinstance(data[key], str) and data[key].startswith("http"):
                        url = data[key]
                        break
                
                if url and self._validate_url(url):
                    parsed = urlparse(url)
                    domain = parsed.netloc.lower()
                    
                    citation = {
                        "url": url,
                        "domain": domain,
                        "source_type": self._classify_source(domain, url, ""),
                        "citation_type": "json_embedded",
                        "context": str(data),
                        "data_points": self._extract_data_points_from_dict(data),
                        "confidence": self._calculate_citation_confidence(domain, "", ""),
                        "extracted_at": datetime.now().isoformat(),
                        "is_trusted": self._is_trusted_domain(domain)
                    }
                    citations.append(citation)
                    
            except (json.JSONDecodeError, KeyError):
                continue
        
        return citations
    
    def _validate_url(self, url: str) -> bool:
        """
        Validate that a URL is properly formatted and accessible.
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def _classify_source(self, domain: str, url: str, context: str) -> str:
        """
        Classify the type of source based on domain and context.
        
        Args:
            domain: Domain name
            url: Full URL
            context: Surrounding context
            
        Returns:
            Source type classification
        """
        domain_lower = domain.lower()
        url_lower = url.lower()
        context_lower = context.lower()
        
        # Hospital/Healthcare systems
        if any(term in domain_lower for term in ["hospital", "health", "medical", "clinic", "cancer", "heart"]):
            return "hospital_website"
        
        # Universities
        if ".edu" in domain_lower or "university" in domain_lower or "college" in domain_lower:
            return "university_website"
        
        # Government
        if ".gov" in domain_lower:
            return "government_database"
        
        # Research databases
        if any(term in domain_lower for term in ["pubmed", "nih", "nejm", "jama", "lancet", "nature", "science"]):
            return "research_database"
        
        # Professional directories
        if any(term in domain_lower for term in ["healthgrades", "vitals", "doximity", "zocdoc", "webmd"]):
            return "professional_directory"
        
        # Professional societies
        if any(term in context_lower for term in ["society", "association", "academy", "college of"]):
            return "professional_society"
        
        # News sources
        if any(term in domain_lower for term in ["news", "times", "post", "journal", "gazette", "tribune"]):
            return "news_article"
        
        # Company websites
        if any(term in domain_lower for term in ["pharma", "device", "bio", "therapeutics", ".com"]):
            return "company_website"
        
        return "other"
    
    def _extract_data_points(self, context: str) -> List[str]:
        """
        Extract specific data points mentioned in the context.
        
        Args:
            context: Text context around citation
            
        Returns:
            List of data point descriptions
        """
        data_points = []
        context_lower = context.lower()
        
        # Position/Title indicators
        if any(term in context_lower for term in ["chief", "director", "chair", "professor", "attending"]):
            data_points.append("position_title")
        
        # Education indicators
        if any(term in context_lower for term in ["graduated", "degree", "residency", "fellowship", "trained"]):
            data_points.append("education_training")
        
        # Certification indicators
        if any(term in context_lower for term in ["board certified", "certification", "licensed"]):
            data_points.append("certification")
        
        # Research indicators
        if any(term in context_lower for term in ["publication", "research", "study", "trial", "grant"]):
            data_points.append("research")
        
        # Affiliation indicators
        if any(term in context_lower for term in ["affiliated", "member", "practices at", "works at"]):
            data_points.append("affiliation")
        
        # Awards/Recognition
        if any(term in context_lower for term in ["award", "honor", "recognition", "top doctor"]):
            data_points.append("recognition")
        
        # Industry relationships
        if any(term in context_lower for term in ["consultant", "advisory", "speaker", "disclosure"]):
            data_points.append("industry_relationship")
        
        return data_points
    
    def _extract_data_points_from_dict(self, data: Dict) -> List[str]:
        """
        Extract data points from a dictionary structure.
        
        Args:
            data: Dictionary containing citation data
            
        Returns:
            List of data point types found
        """
        data_points = []
        
        # Convert dict to string for analysis
        data_str = json.dumps(data).lower()
        
        # Use same extraction logic
        return self._extract_data_points(data_str)
    
    def _calculate_citation_confidence(self, domain: str, source_type: str, context: str) -> float:
        """
        Calculate confidence score for a citation.
        
        Args:
            domain: Domain of the source
            source_type: Type of source
            context: Citation context
            
        Returns:
            Confidence score between 0 and 1
        """
        base_confidence = 0.5
        
        # Source credibility weight
        if source_type in self.credibility_weights:
            base_confidence = self.credibility_weights[source_type]
        
        # Trusted domain bonus
        if self._is_trusted_domain(domain):
            base_confidence = min(1.0, base_confidence + 0.15)
        
        # Context quality factors
        if len(context) > 100:  # Substantial context
            base_confidence = min(1.0, base_confidence + 0.05)
        
        # Specific data indicators
        if any(term in context.lower() for term in ["verified", "confirmed", "official"]):
            base_confidence = min(1.0, base_confidence + 0.1)
        
        # Negative indicators
        if any(term in context.lower() for term in ["unverified", "claimed", "alleged", "reportedly"]):
            base_confidence = max(0.1, base_confidence - 0.2)
        
        return round(base_confidence, 2)
    
    def _is_trusted_domain(self, domain: str) -> bool:
        """
        Check if domain is in trusted list.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if trusted
        """
        return any(trusted in domain for trusted in self.trusted_domains)
    
    def _deduplicate_citations(self, citations: List[Dict]) -> List[Dict]:
        """
        Remove duplicate citations, keeping the highest confidence version.
        
        Args:
            citations: List of citations
            
        Returns:
            Deduplicated list
        """
        unique = {}
        
        for citation in citations:
            url = citation["url"]
            if url not in unique or citation["confidence"] > unique[url]["confidence"]:
                unique[url] = citation
        
        return list(unique.values())
    
    def _enhance_citations(self, citations: List[Dict]) -> List[Dict]:
        """
        Enhance citations with additional metadata.
        
        Args:
            citations: List of citations
            
        Returns:
            Enhanced citations
        """
        for citation in citations:
            # Add page title extraction (if in context)
            title_match = re.search(r'<title>(.*?)</title>', citation.get("context", ""), re.IGNORECASE)
            if title_match:
                citation["page_title"] = title_match.group(1)
            
            # Add date extraction if present
            date_patterns = [
                r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',
                r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4}\b',
                r'\b\d{4}\b'
            ]
            
            for pattern in date_patterns:
                date_match = re.search(pattern, citation.get("context", ""))
                if date_match:
                    citation["content_date"] = date_match.group(0)
                    break
            
            # Mark if citation contains critical data
            critical_terms = ["npi", "license", "board certification", "coi", "disclosure", "payment"]
            citation["has_critical_data"] = any(
                term in citation.get("context", "").lower() 
                for term in critical_terms
            )
        
        return citations
    
    def generate_citation_report(self, citations: List[Dict]) -> Dict[str, Any]:
        """
        Generate a summary report of citations.
        
        Args:
            citations: List of citations
            
        Returns:
            Summary report dictionary
        """
        report = {
            "total_citations": len(citations),
            "unique_domains": len(set(c["domain"] for c in citations)),
            "source_types": {},
            "confidence_distribution": {
                "high": 0,      # >= 0.8
                "medium": 0,    # 0.6 - 0.79
                "low": 0        # < 0.6
            },
            "trusted_sources": 0,
            "data_coverage": {},
            "top_sources": []
        }
        
        # Count source types
        for c in citations:
            source_type = c.get("source_type", "other")
            report["source_types"][source_type] = report["source_types"].get(source_type, 0) + 1
            
            # Confidence distribution
            conf = c.get("confidence", 0)
            if conf >= 0.8:
                report["confidence_distribution"]["high"] += 1
            elif conf >= 0.6:
                report["confidence_distribution"]["medium"] += 1
            else:
                report["confidence_distribution"]["low"] += 1
            
            # Trusted sources
            if c.get("is_trusted", False):
                report["trusted_sources"] += 1
            
            # Data coverage
            for dp in c.get("data_points", []):
                report["data_coverage"][dp] = report["data_coverage"].get(dp, 0) + 1
        
        # Top sources by confidence
        report["top_sources"] = sorted(
            citations,
            key=lambda x: x.get("confidence", 0),
            reverse=True
        )[:5]
        
        # Clean top sources for report
        report["top_sources"] = [
            {
                "url": s["url"],
                "domain": s["domain"],
                "confidence": s["confidence"],
                "data_points": s["data_points"]
            }
            for s in report["top_sources"]
        ]
        
        return report