#!/usr/bin/env python3
"""
Single Provider Web Search Script
DA-173: Provider Profile Web Enrichment POC

Usage:
    python scrape_single_provider.py --name "Dr. John Smith" --institution "Mayo Clinic" --specialty "Cardiology"
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.provider_scraper import ProviderProfileScraper
from src.logger import setup_logger

def main():
    """Main execution function for single provider web search."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Web search for a single healthcare provider profile"
    )
    
    # Required arguments
    parser.add_argument(
        "--name",
        type=str,
        required=True,
        help="Provider's full name (e.g., 'Dr. John Smith')"
    )
    
    # Optional arguments
    parser.add_argument(
        "--institution",
        type=str,
        help="Hospital or institution affiliation"
    )
    
    parser.add_argument(
        "--specialty",
        type=str,
        help="Medical specialty (e.g., 'Cardiology', 'Oncology')"
    )
    
    parser.add_argument(
        "--npi",
        type=str,
        help="National Provider Identifier (10-digit number)"
    )
    
    parser.add_argument(
        "--location",
        type=str,
        help="Geographic location (e.g., 'Boston, MA')"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to configuration file (default: config/config.yaml)"
    )
    
    parser.add_argument(
        "--prompts",
        type=str,
        default="config/prompts.yaml",
        help="Path to prompts file (default: config/prompts.yaml)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Custom output file path (optional)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print configuration without executing search"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_config = {
        "level": "DEBUG" if args.verbose else "INFO",
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "log_rotation": "size",
        "max_log_size_mb": 10,
        "backup_count": 5
    }
    
    logger = setup_logger("SingleProviderSearch", log_config)
    
    logger.info("=" * 60)
    logger.info("Provider Profile Web Search - Single Provider Mode")
    logger.info("=" * 60)
    
    # Log search parameters
    logger.info(f"Provider Name: {args.name}")
    if args.institution:
        logger.info(f"Institution: {args.institution}")
    if args.specialty:
        logger.info(f"Specialty: {args.specialty}")
    if args.npi:
        logger.info(f"NPI: {args.npi}")
    if args.location:
        logger.info(f"Location: {args.location}")
    
    # Dry run mode
    if args.dry_run:
        logger.info("\n*** DRY RUN MODE - No API calls will be made ***")
        
        search_params = {
            "name": args.name,
            "institution": args.institution,
            "specialty": args.specialty,
            "npi": args.npi,
            "location": args.location
        }
        
        logger.info("\nSearch parameters:")
        logger.info(json.dumps(search_params, indent=2))
        
        logger.info("\nConfiguration files:")
        logger.info(f"  Config: {args.config}")
        logger.info(f"  Prompts: {args.prompts}")
        
        if args.output:
            logger.info(f"\nOutput will be saved to: {args.output}")
        else:
            logger.info("\nOutput will be saved to default location in data/output/")
        
        return
    
    try:
        # Initialize scraper
        logger.info("\nInitializing provider web search engine...")
        scraper = ProviderProfileScraper(
            config_path=args.config,
            prompts_path=args.prompts
        )
        
        # Execute search
        logger.info("\nExecuting web search...")
        logger.info("This may take 30-60 seconds depending on the amount of information available...")
        
        result = scraper.scrape_provider(
            name=args.name,
            institution=args.institution,
            specialty=args.specialty,
            npi=args.npi,
            location=args.location
        )
        
        # Check for errors
        if "error" in result["metadata"]:
            logger.error(f"\nError during search: {result['metadata']['error']}")
            return 1
        
        # Display summary
        logger.info("\n" + "=" * 60)
        logger.info("SEARCH COMPLETE")
        logger.info("=" * 60)
        
        # Profile summary
        profile = result["profile"]
        logger.info("\nProfile Summary:")
        
        if "basic" in profile:
            basic = profile["basic"]
            logger.info(f"  Name: {basic.get('full_name', 'Not found')}")
            logger.info(f"  Specialty: {basic.get('primary_specialty', 'Not found')}")
            if basic.get("npi"):
                logger.info(f"  NPI: {basic['npi']}")
        
        if "professional" in profile and "current_positions" in profile["professional"]:
            positions = profile["professional"]["current_positions"]
            logger.info(f"\n  Current Positions ({len(positions)}):")
            for pos in positions[:3]:  # Show first 3
                logger.info(f"    - {pos.get('title', 'Unknown')} at {pos.get('institution', 'Unknown')}")
        
        # Citations summary
        citations = result["citations"]
        logger.info(f"\nCitations Summary:")
        logger.info(f"  Total sources found: {len(citations)}")
        
        # Count by source type
        source_types = {}
        for c in citations:
            st = c.get("source_type", "other")
            source_types[st] = source_types.get(st, 0) + 1
        
        logger.info("  Sources by type:")
        for st, count in sorted(source_types.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    - {st}: {count}")
        
        # Quality metrics
        metadata = result["metadata"]
        logger.info(f"\nQuality Metrics:")
        logger.info(f"  Overall Confidence: {metadata.get('overall_confidence', 0):.1%}")
        logger.info(f"  Field Completeness: {metadata.get('field_completeness', 0):.1%}")
        logger.info(f"  Processing Time: {metadata.get('processing_time_seconds', 0):.1f} seconds")
        
        if metadata.get("manual_review_required"):
            logger.warning(f"\n  ⚠ Manual Review Required:")
            for reason in metadata.get("review_reasons", []):
                logger.warning(f"    - {reason}")
        
        # Save custom output if specified
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"\nCustom output saved to: {output_path}")
        
        # Show default output location
        else:
            # Find the output file (it was already saved by the scraper)
            output_dir = Path(scraper.output_dir) / "profiles"
            output_files = list(output_dir.glob(f"*{result['metadata']['request_id']}.json"))
            
            if output_files:
                logger.info(f"\nOutput saved to: {output_files[0]}")
                
                # Also show citations file
                citations_dir = Path(scraper.output_dir) / "citations"
                citation_files = list(citations_dir.glob(f"*{result['metadata']['request_id']}_citations.json"))
                if citation_files:
                    logger.info(f"Citations saved to: {citation_files[0]}")
        
        # Show sample of high-confidence data points
        logger.info("\n" + "=" * 60)
        logger.info("HIGH-CONFIDENCE DATA POINTS (Sample)")
        logger.info("=" * 60)
        
        high_conf_citations = [c for c in citations if c.get("confidence", 0) >= 0.8][:3]
        
        for i, citation in enumerate(high_conf_citations, 1):
            logger.info(f"\n{i}. Source: {citation['domain']}")
            logger.info(f"   URL: {citation['url'][:100]}...")
            logger.info(f"   Confidence: {citation['confidence']:.1%}")
            logger.info(f"   Data Points: {', '.join(citation.get('data_points', []))}")
            
            # Show snippet of context
            context = citation.get("context", "")[:200]
            if context:
                logger.info(f"   Context: {context}...")
        
        logger.info("\n" + "=" * 60)
        logger.info("Search completed successfully!")
        logger.info("=" * 60)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"\nConfiguration file not found: {e}")
        logger.error("Please ensure config/config.yaml and config/prompts.yaml exist")
        return 1
        
    except ValueError as e:
        logger.error(f"\nConfiguration error: {e}")
        logger.error("Please check your OpenAI API key is set correctly")
        return 1
        
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())