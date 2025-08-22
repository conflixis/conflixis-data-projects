"""
Core Provider Profile Web Search
DA-173: Provider Profile Web Enrichment POC
"""

import os
import re
import json
import time
import uuid
import yaml
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

from openai import OpenAI
from dotenv import load_dotenv

from .profile_parser import ProfileParser
from .citation_extractor import CitationExtractor
from .logger import setup_logger

# Load environment variables
load_dotenv()

class ProviderProfileScraper:
    """
    Main web search class for extracting healthcare provider profiles from web sources.
    Uses OpenAI's web search capabilities to gather comprehensive, cited information.
    """
    
    def __init__(self, config_path: str = "config/config.yaml", prompts_path: str = "config/prompts.yaml"):
        """
        Initialize the scraper with configuration.
        
        Args:
            config_path: Path to configuration file
            prompts_path: Path to prompts file
        """
        # Load configurations
        self.config = self._load_yaml(config_path)
        self.prompts = self._load_yaml(prompts_path)
        
        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY") or self.config["openai"]["api_key"]
        if not api_key or api_key == "${OPENAI_API_KEY}":
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)
        
        # Initialize components
        self.parser = ProfileParser(self.config)
        self.citation_extractor = CitationExtractor(self.config)
        self.logger = setup_logger("ProviderScraper", self.config["logging"])
        
        # Setup output directories
        self._setup_directories()
        
    def _load_yaml(self, path: str) -> Dict:
        """Load YAML configuration file."""
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_directories(self):
        """Create necessary output directories."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.config["storage"]["create_timestamped_folders"]:
            self.output_dir = Path(self.config["storage"]["output_dir"]) / timestamp
            self.log_dir = Path(self.config["storage"]["log_dir"]) / timestamp
        else:
            self.output_dir = Path(self.config["storage"]["output_dir"])
            self.log_dir = Path(self.config["storage"]["log_dir"])
        
        # Create directories
        (self.output_dir / "profiles").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "citations").mkdir(parents=True, exist_ok=True)
        (self.log_dir).mkdir(parents=True, exist_ok=True)
        
    def scrape_provider(
        self, 
        name: str, 
        institution: str = None,
        specialty: str = None,
        npi: str = None,
        location: str = None
    ) -> Dict[str, Any]:
        """
        Core function to scrape a single provider's profile.
        
        Args:
            name: Provider's full name
            institution: Hospital or institution affiliation
            specialty: Medical specialty
            npi: National Provider Identifier (optional)
            location: Geographic location (optional)
            
        Returns:
            Dictionary containing profile, citations, and metadata
        """
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        self.logger.info(f"Starting scrape for provider: {name} (Request ID: {request_id})")
        
        try:
            # Build search query
            provider_info = self._build_provider_query(name, institution, specialty, npi, location)
            
            # Log the request
            self._log_request(request_id, provider_info)
            
            # Execute web search with OpenAI
            raw_response = self._execute_web_search(provider_info, request_id)
            
            # Parse the response
            parsed_profile = self.parser.parse_response(raw_response)
            
            # Extract citations
            citations = self.citation_extractor.extract_citations(raw_response)
            
            # Calculate metadata
            metadata = self._generate_metadata(
                request_id=request_id,
                start_time=start_time,
                provider_info=provider_info,
                parsed_profile=parsed_profile,
                citations=citations,
                raw_response=raw_response
            )
            
            # Validate and assess quality
            quality_assessment = self._assess_quality(parsed_profile, citations)
            metadata.update(quality_assessment)
            
            # Prepare final output
            result = {
                "profile": parsed_profile,
                "citations": citations,
                "metadata": metadata
            }
            
            # Save outputs
            self._save_outputs(result, name, request_id)
            
            self.logger.info(f"Successfully completed scrape for {name} (Request ID: {request_id})")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error scraping provider {name}: {str(e)}", exc_info=True)
            
            # Return partial result with error
            return {
                "profile": {},
                "citations": [],
                "metadata": {
                    "request_id": request_id,
                    "scraped_at": datetime.now().isoformat(),
                    "error": str(e),
                    "processing_time_seconds": time.time() - start_time
                }
            }
    
    def _build_provider_query(
        self, 
        name: str, 
        institution: str = None,
        specialty: str = None,
        npi: str = None,
        location: str = None
    ) -> str:
        """Build the provider information string for the query."""
        parts = [f"Name: {name}"]
        
        if institution:
            parts.append(f"Institution: {institution}")
        if specialty:
            parts.append(f"Specialty: {specialty}")
        if npi:
            parts.append(f"NPI: {npi}")
        if location:
            parts.append(f"Location: {location}")
            
        return "\n".join(parts)
    
    def _execute_web_search(self, provider_info: str, request_id: str) -> str:
        """
        Execute web search using OpenAI with web_search_preview tool.
        """
        self.logger.debug(f"Executing web search for request {request_id}")
        
        # Prepare input messages in the correct format
        input_messages = [
            {
                "role": "system",
                "content": [
                    {"type": "input_text", "text": self.prompts["system_prompt"]}
                ]
            },
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": provider_info}
                ]
            }
        ]
        
        # Execute search with retries
        max_retries = self.config["quality"]["max_retry_attempts"]
        retry_delay = self.config["quality"]["retry_delay_seconds"]
        
        for attempt in range(max_retries):
            try:
                response = self.client.responses.create(
                    model=self.config["openai"]["model"],
                    input=input_messages,
                    text={
                        "format": {"type": "text"},
                        "verbosity": "medium"
                    },
                    tools=[
                        {
                            "type": "web_search_preview",
                            "user_location": {
                                "type": "approximate",
                                "country": "US"
                            },
                            "search_context_size": self.config["web_search"]["search_context_size"]
                        }
                    ],
                    tool_choice={"type": "web_search_preview"},
                    temperature=self.config["openai"]["temperature"],
                    max_output_tokens=self.config["openai"]["max_tokens"],
                    top_p=self.config["openai"]["top_p"],
                    stream=False,
                    store=False  # Don't store for POC
                )
                
                # Extract the response content
                raw_text = response.output_text
                
                # Clean any markdown formatting
                raw_text = re.sub(r"```(json)?\s*", "", raw_text)
                raw_text = re.sub(r"\s*```", "", raw_text)
                
                # Log the full response if configured
                if self.config["logging"]["capture_full_responses"]:
                    self._log_response(request_id, raw_text)
                
                return raw_text
                
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def _generate_metadata(
        self,
        request_id: str,
        start_time: float,
        provider_info: str,
        parsed_profile: Dict,
        citations: List[Dict],
        raw_response: str
    ) -> Dict[str, Any]:
        """Generate metadata for the scraping request."""
        
        # Calculate field completeness
        field_completeness = self._calculate_completeness(parsed_profile)
        
        # Calculate overall confidence
        overall_confidence = self._calculate_confidence(citations)
        
        metadata = {
            "request_id": request_id,
            "scraped_at": datetime.now().isoformat(),
            "scrape_version": "1.0.0",
            "search_queries_used": [provider_info],
            "total_sources_found": len(citations),
            "sources_used": len([c for c in citations if c.get("confidence", 0) >= 
                                self.config["quality"]["min_confidence_threshold"]]),
            "sources_excluded": len([c for c in citations if c.get("confidence", 0) < 
                                   self.config["quality"]["min_confidence_threshold"]]),
            "overall_confidence": overall_confidence,
            "field_completeness": field_completeness,
            "processing_time_seconds": round(time.time() - start_time, 2),
            "api_calls_made": 1,
            "api_tokens_used": len(raw_response.split()) * 1.3,  # Rough estimate
            "errors": [],
            "warnings": []
        }
        
        return metadata
    
    def _calculate_completeness(self, profile: Dict) -> float:
        """Calculate the completeness score of the profile."""
        weights = self.config["field_priority"]
        total_weight = 0
        filled_weight = 0
        
        def check_field(obj, path, weight):
            nonlocal total_weight, filled_weight
            total_weight += weight
            
            parts = path.split('.')
            current = obj
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return
            
            if current and (
                (isinstance(current, str) and current.strip()) or
                (isinstance(current, list) and len(current) > 0) or
                (isinstance(current, dict) and len(current) > 0)
            ):
                filled_weight += weight
        
        # Check each field
        for field_path, weight in weights.items():
            check_field(profile, field_path, weight)
        
        return round(filled_weight / total_weight if total_weight > 0 else 0, 2)
    
    def _calculate_confidence(self, citations: List[Dict]) -> float:
        """Calculate overall confidence score based on citations."""
        if not citations:
            return 0.0
        
        confidences = [c.get("confidence", 0) for c in citations]
        
        # Weighted average based on source credibility
        credibility_weights = self.config["source_credibility"]
        weighted_sum = 0
        total_weight = 0
        
        for citation in citations:
            source_type = citation.get("source_type", "").lower().replace(" ", "_")
            weight = credibility_weights.get(source_type, 0.5)
            confidence = citation.get("confidence", 0)
            
            weighted_sum += confidence * weight
            total_weight += weight
        
        return round(weighted_sum / total_weight if total_weight > 0 else 0, 2)
    
    def _assess_quality(self, profile: Dict, citations: List[Dict]) -> Dict:
        """Assess the quality of the scraped data."""
        assessment = {
            "manual_review_required": False,
            "review_reasons": []
        }
        
        # Check confidence threshold
        overall_confidence = self._calculate_confidence(citations)
        if overall_confidence < self.config["manual_review"]["confidence_threshold"]:
            assessment["manual_review_required"] = True
            assessment["review_reasons"].append(f"Low confidence: {overall_confidence}")
        
        # Check completeness threshold
        completeness = self._calculate_completeness(profile)
        if completeness < self.config["manual_review"]["completeness_threshold"]:
            assessment["manual_review_required"] = True
            assessment["review_reasons"].append(f"Low completeness: {completeness}")
        
        # Check for high-value titles
        high_value_titles = self.config["manual_review"]["high_value_titles"]
        if "professional" in profile and "current_positions" in profile["professional"]:
            for position in profile["professional"]["current_positions"]:
                title = position.get("title", "").lower()
                if any(hvt.lower() in title for hvt in high_value_titles):
                    assessment["manual_review_required"] = True
                    assessment["review_reasons"].append(f"High-value position: {position['title']}")
                    break
        
        return assessment
    
    def _save_outputs(self, result: Dict, provider_name: str, request_id: str):
        """Save the scraping results to files."""
        # Sanitize filename
        safe_name = re.sub(r'[^\w\s-]', '', provider_name).strip().replace(' ', '_')
        
        # Save profile JSON
        profile_path = self.output_dir / "profiles" / f"{safe_name}_{request_id}.json"
        with open(profile_path, 'w', encoding='utf-8') as f:
            if self.config["storage"]["pretty_print_json"]:
                json.dump(result, f, indent=2, ensure_ascii=False)
            else:
                json.dump(result, f, ensure_ascii=False)
        
        self.logger.info(f"Saved profile to {profile_path}")
        
        # Save citations separately
        citations_path = self.output_dir / "citations" / f"{safe_name}_{request_id}_citations.json"
        with open(citations_path, 'w', encoding='utf-8') as f:
            json.dump(result["citations"], f, indent=2, ensure_ascii=False)
        
        self.logger.debug(f"Saved citations to {citations_path}")
    
    def _log_request(self, request_id: str, provider_info: str):
        """Log the API request."""
        log_entry = {
            "request_id": request_id,
            "timestamp": datetime.now().isoformat(),
            "input": provider_info,
            "config": {
                "model": self.config["openai"]["model"],
                "temperature": self.config["openai"]["temperature"],
                "max_tokens": self.config["openai"]["max_tokens"]
            }
        }
        
        log_path = self.log_dir / f"request_{request_id}.json"
        with open(log_path, 'w') as f:
            json.dump(log_entry, f, indent=2)
    
    def _log_response(self, request_id: str, response: str):
        """Log the API response."""
        log_path = self.log_dir / f"response_{request_id}.txt"
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(response)
    
    def scrape_bulk(self, providers: List[Dict], checkpoint_file: str = None) -> List[Dict]:
        """
        Scrape multiple providers with checkpointing.
        
        Args:
            providers: List of provider dictionaries with name, institution, etc.
            checkpoint_file: Path to checkpoint file for resuming
            
        Returns:
            List of scraping results
        """
        results = []
        checkpoint_path = Path(checkpoint_file) if checkpoint_file else \
                         Path(self.config["storage"]["checkpoint_dir"]) / "checkpoint.json"
        
        # Load checkpoint if exists
        start_index = 0
        if checkpoint_path.exists():
            with open(checkpoint_path, 'r') as f:
                checkpoint = json.load(f)
                start_index = checkpoint.get("last_completed_index", 0) + 1
                self.logger.info(f"Resuming from index {start_index}")
        
        # Process providers
        for i, provider in enumerate(providers[start_index:], start=start_index):
            self.logger.info(f"Processing provider {i+1}/{len(providers)}: {provider.get('name')}")
            
            # Scrape provider
            result = self.scrape_provider(
                name=provider.get("name"),
                institution=provider.get("institution"),
                specialty=provider.get("specialty"),
                npi=provider.get("npi"),
                location=provider.get("location")
            )
            
            results.append(result)
            
            # Save checkpoint
            if (i + 1) % self.config["processing"]["checkpoint_frequency"] == 0:
                checkpoint = {
                    "last_completed_index": i,
                    "timestamp": datetime.now().isoformat(),
                    "total_providers": len(providers)
                }
                checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                with open(checkpoint_path, 'w') as f:
                    json.dump(checkpoint, f, indent=2)
                self.logger.info(f"Checkpoint saved at index {i}")
            
            # Rate limiting
            if (i + 1) < len(providers):
                time.sleep(60 / self.config["processing"]["rate_limit_per_minute"])
        
        self.logger.info(f"Completed bulk scraping of {len(results)} providers")
        return results