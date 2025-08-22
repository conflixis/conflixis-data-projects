#!/usr/bin/env python3
"""
Test Script for Provider Web Search
DA-173: Provider Profile Web Enrichment POC

This script tests the web search functionality with a known provider.
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.provider_scraper import ProviderProfileScraper
from src.logger import setup_logger

def test_single_provider():
    """Test web search for a single well-known provider."""
    
    print("=" * 60)
    print("Provider Web Search Test")
    print("=" * 60)
    
    # Setup logging
    log_config = {
        "level": "INFO",
        "log_format": "%(asctime)s - %(levelname)s - %(message)s",
        "log_rotation": "size",
        "max_log_size_mb": 10,
        "backup_count": 5
    }
    
    logger = setup_logger("TestWebSearch", log_config)
    
    try:
        # Initialize scraper
        logger.info("Initializing web search engine...")
        scraper = ProviderProfileScraper()
        
        # Test with a well-known provider
        test_provider = {
            "name": "Dr. Anthony Fauci",
            "institution": "National Institute of Allergy and Infectious Diseases",
            "specialty": "Immunology",
            "location": "Bethesda, MD"
        }
        
        logger.info(f"\nTesting with provider: {test_provider['name']}")
        logger.info(f"Institution: {test_provider['institution']}")
        logger.info(f"Specialty: {test_provider['specialty']}")
        
        # Execute search
        logger.info("\nExecuting web search (this may take 30-60 seconds)...")
        
        result = scraper.scrape_provider(
            name=test_provider["name"],
            institution=test_provider["institution"],
            specialty=test_provider["specialty"],
            location=test_provider["location"]
        )
        
        # Validate result structure
        assert "profile" in result, "Result missing 'profile' key"
        assert "citations" in result, "Result missing 'citations' key"
        assert "metadata" in result, "Result missing 'metadata' key"
        
        # Check for errors
        if "error" in result["metadata"]:
            logger.error(f"Search failed: {result['metadata']['error']}")
            return False
        
        # Display results
        logger.info("\n" + "=" * 60)
        logger.info("TEST RESULTS")
        logger.info("=" * 60)
        
        # Profile validation
        profile = result["profile"]
        logger.info("\n✓ Profile Structure:")
        
        expected_sections = ["basic", "professional", "education", "research"]
        found_sections = [s for s in expected_sections if s in profile and profile[s]]
        
        logger.info(f"  Found {len(found_sections)}/{len(expected_sections)} expected sections")
        for section in found_sections:
            logger.info(f"  - {section}: ✓")
        
        # Basic info
        if "basic" in profile:
            basic = profile["basic"]
            logger.info("\n✓ Basic Information:")
            logger.info(f"  Name: {basic.get('full_name', 'Not found')}")
            logger.info(f"  Specialty: {basic.get('primary_specialty', 'Not found')}")
            if basic.get("credentials"):
                logger.info(f"  Credentials: {', '.join(basic['credentials'])}")
        
        # Citations validation
        citations = result["citations"]
        logger.info(f"\n✓ Citations:")
        logger.info(f"  Total sources: {len(citations)}")
        
        if citations:
            # Check citation structure
            required_fields = ["url", "domain", "source_type", "confidence"]
            first_citation = citations[0]
            
            missing_fields = [f for f in required_fields if f not in first_citation]
            if missing_fields:
                logger.warning(f"  Citation missing fields: {missing_fields}")
            else:
                logger.info("  Citation structure: ✓")
            
            # Show source distribution
            source_types = {}
            for c in citations:
                st = c.get("source_type", "other")
                source_types[st] = source_types.get(st, 0) + 1
            
            logger.info("  Source types:")
            for st, count in sorted(source_types.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    - {st}: {count}")
        
        # Metadata validation
        metadata = result["metadata"]
        logger.info(f"\n✓ Metadata:")
        logger.info(f"  Request ID: {metadata.get('request_id', 'Missing')}")
        logger.info(f"  Confidence: {metadata.get('overall_confidence', 0):.1%}")
        logger.info(f"  Completeness: {metadata.get('field_completeness', 0):.1%}")
        logger.info(f"  Processing time: {metadata.get('processing_time_seconds', 0):.1f}s")
        
        # Quality checks
        logger.info(f"\n✓ Quality Checks:")
        
        confidence = metadata.get('overall_confidence', 0)
        if confidence >= 0.7:
            logger.info(f"  Confidence: PASS ({confidence:.1%})")
        else:
            logger.warning(f"  Confidence: LOW ({confidence:.1%})")
        
        completeness = metadata.get('field_completeness', 0)
        if completeness >= 0.6:
            logger.info(f"  Completeness: PASS ({completeness:.1%})")
        else:
            logger.warning(f"  Completeness: LOW ({completeness:.1%})")
        
        if len(citations) >= 3:
            logger.info(f"  Citations: PASS ({len(citations)} sources)")
        else:
            logger.warning(f"  Citations: LOW ({len(citations)} sources)")
        
        # Save test output
        test_output_dir = Path("test_output")
        test_output_dir.mkdir(exist_ok=True)
        
        output_file = test_output_dir / f"test_result_{metadata['request_id']}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n✓ Test output saved to: {output_file}")
        
        # Overall test result
        logger.info("\n" + "=" * 60)
        
        test_passed = (
            confidence >= 0.5 and
            completeness >= 0.4 and
            len(citations) >= 1 and
            len(found_sections) >= 2
        )
        
        if test_passed:
            logger.info("TEST PASSED ✓")
            logger.info("Web search functionality is working correctly")
        else:
            logger.warning("TEST COMPLETED WITH WARNINGS")
            logger.warning("Web search returned results but quality is lower than expected")
        
        logger.info("=" * 60)
        
        return test_passed
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        return False


def test_components():
    """Test individual components."""
    
    print("\n" + "=" * 60)
    print("Component Tests")
    print("=" * 60)
    
    # Test imports
    print("\n1. Testing imports...")
    try:
        from src.provider_scraper import ProviderProfileScraper
        from src.profile_parser import ProfileParser
        from src.citation_extractor import CitationExtractor
        from src.logger import setup_logger, AuditLogger, MetricsLogger
        print("   ✓ All imports successful")
    except ImportError as e:
        print(f"   ✗ Import failed: {e}")
        return False
    
    # Test configuration loading
    print("\n2. Testing configuration...")
    try:
        import yaml
        with open("config/config.yaml", 'r') as f:
            config = yaml.safe_load(f)
        assert "openai" in config
        assert "web_search" in config
        print("   ✓ Configuration loaded successfully")
    except Exception as e:
        print(f"   ✗ Configuration error: {e}")
        return False
    
    # Test prompts loading
    print("\n3. Testing prompts...")
    try:
        with open("config/prompts.yaml", 'r') as f:
            prompts = yaml.safe_load(f)
        assert "system_prompt" in prompts
        print("   ✓ Prompts loaded successfully")
    except Exception as e:
        print(f"   ✗ Prompts error: {e}")
        return False
    
    # Test citation extractor
    print("\n4. Testing citation extractor...")
    try:
        extractor = CitationExtractor(config)
        test_text = """
        Dr. Smith is the Chief of Cardiology at General Hospital 
        [Source: https://generalhospital.org/staff/smith].
        He received his MD from Harvard Medical School in 2000
        [Source: https://hms.harvard.edu/alumni/smith].
        """
        citations = extractor.extract_citations(test_text)
        assert len(citations) == 2
        assert all("url" in c for c in citations)
        print(f"   ✓ Citation extractor working ({len(citations)} citations found)")
    except Exception as e:
        print(f"   ✗ Citation extractor error: {e}")
        return False
    
    # Test profile parser
    print("\n5. Testing profile parser...")
    try:
        parser = ProfileParser(config)
        test_json = json.dumps({
            "name": "Dr. John Smith",
            "specialty": "Cardiology",
            "positions": [{"title": "Chief", "institution": "Hospital"}]
        })
        profile = parser.parse_response(test_json)
        assert profile is not None
        print("   ✓ Profile parser working")
    except Exception as e:
        print(f"   ✗ Profile parser error: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("All component tests passed ✓")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    # Run component tests first
    if not test_components():
        print("\nComponent tests failed. Please fix issues before running full test.")
        sys.exit(1)
    
    # Check for API key
    import os
    if not os.getenv("OPENAI_API_KEY"):
        print("\n⚠ Warning: OPENAI_API_KEY not set in environment")
        print("Please set your OpenAI API key to run the full test:")
        print("  export OPENAI_API_KEY='your-api-key'")
        print("\nSkipping live web search test.")
        sys.exit(0)
    
    # Run full test
    print("\nRunning live web search test...")
    success = test_single_provider()
    
    sys.exit(0 if success else 1)