#!/usr/bin/env python3
"""
Bulk Provider Web Search Script
DA-173: Provider Profile Web Enrichment POC

Usage:
    python scrape_bulk_providers.py --input providers.csv
    python scrape_bulk_providers.py --input providers.json --resume
"""

import argparse
import json
import csv
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.provider_scraper import ProviderProfileScraper
from src.logger import setup_logger, MetricsLogger

def load_providers_csv(file_path: str) -> List[Dict[str, str]]:
    """
    Load providers from CSV file.
    
    Expected columns: name, institution, specialty, npi, location
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        List of provider dictionaries
    """
    providers = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            provider = {
                "name": row.get("name", "").strip(),
                "institution": row.get("institution", "").strip() or None,
                "specialty": row.get("specialty", "").strip() or None,
                "npi": row.get("npi", "").strip() or None,
                "location": row.get("location", "").strip() or None
            }
            
            # Skip if no name
            if provider["name"]:
                providers.append(provider)
    
    return providers


def load_providers_json(file_path: str) -> List[Dict[str, str]]:
    """
    Load providers from JSON file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        List of provider dictionaries
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both array and object with providers key
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and "providers" in data:
        return data["providers"]
    else:
        raise ValueError("JSON file must contain array or object with 'providers' key")


def main():
    """Main execution function for bulk provider web search."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Bulk web search for healthcare provider profiles"
    )
    
    # Required arguments
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input file path (CSV or JSON)"
    )
    
    # Optional arguments
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--prompts",
        type=str,
        default="config/prompts.yaml",
        help="Path to prompts file"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Custom output directory"
    )
    
    parser.add_argument(
        "--checkpoint",
        type=str,
        help="Custom checkpoint file path"
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of providers to process"
    )
    
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Start processing from specific index"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show providers to process without executing"
    )
    
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=30,
        help="Requests per minute (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_config = {
        "level": "DEBUG" if args.verbose else "INFO",
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "log_rotation": "size",
        "max_log_size_mb": 50,
        "backup_count": 10
    }
    
    logger = setup_logger("BulkProviderSearch", log_config)
    
    logger.info("=" * 60)
    logger.info("Provider Profile Web Search - Bulk Mode")
    logger.info("=" * 60)
    
    # Load providers
    input_path = Path(args.input)
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    try:
        logger.info(f"Loading providers from: {input_path}")
        
        if input_path.suffix.lower() == ".csv":
            providers = load_providers_csv(input_path)
        elif input_path.suffix.lower() == ".json":
            providers = load_providers_json(input_path)
        else:
            logger.error("Input file must be CSV or JSON")
            return 1
        
        logger.info(f"Loaded {len(providers)} providers")
        
    except Exception as e:
        logger.error(f"Error loading providers: {e}")
        return 1
    
    # Apply limit if specified
    if args.limit:
        providers = providers[:args.limit]
        logger.info(f"Limited to {len(providers)} providers")
    
    # Apply start index
    if args.start_index > 0:
        providers = providers[args.start_index:]
        logger.info(f"Starting from index {args.start_index}")
    
    # Dry run mode
    if args.dry_run:
        logger.info("\n*** DRY RUN MODE - No API calls will be made ***")
        logger.info(f"\nProviders to process: {len(providers)}")
        
        # Show first 5 providers
        logger.info("\nFirst 5 providers:")
        for i, provider in enumerate(providers[:5], 1):
            logger.info(f"{i}. {provider['name']}")
            if provider.get('institution'):
                logger.info(f"   Institution: {provider['institution']}")
            if provider.get('specialty'):
                logger.info(f"   Specialty: {provider['specialty']}")
        
        if len(providers) > 5:
            logger.info(f"... and {len(providers) - 5} more")
        
        # Calculate estimated time
        estimated_time = len(providers) * (60 / args.rate_limit + 30)  # Rate limit + processing
        logger.info(f"\nEstimated processing time: {estimated_time/60:.1f} minutes")
        
        return 0
    
    # Initialize components
    try:
        logger.info("\nInitializing provider web search engine...")
        scraper = ProviderProfileScraper(
            config_path=args.config,
            prompts_path=args.prompts
        )
        
        # Override rate limit if specified
        if args.rate_limit:
            scraper.config["processing"]["rate_limit_per_minute"] = args.rate_limit
        
        # Setup custom output directory if specified
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            scraper.output_dir = output_dir
            logger.info(f"Using custom output directory: {output_dir}")
        
        # Setup metrics logger
        metrics_file = scraper.output_dir / "metrics.json"
        metrics_logger = MetricsLogger(metrics_file)
        
    except Exception as e:
        logger.error(f"Error initializing scraper: {e}")
        return 1
    
    # Checkpoint handling
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else \
                     scraper.output_dir / "checkpoint.json"
    
    start_index = 0
    completed_providers = []
    
    if args.resume and checkpoint_path.exists():
        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint = json.load(f)
            
            start_index = checkpoint.get("last_completed_index", -1) + 1
            completed_providers = checkpoint.get("completed_providers", [])
            
            logger.info(f"Resuming from checkpoint (index {start_index})")
            logger.info(f"Previously completed: {len(completed_providers)} providers")
            
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}")
            logger.info("Starting from beginning")
    
    # Process providers
    logger.info("\n" + "=" * 60)
    logger.info("STARTING BULK PROCESSING")
    logger.info("=" * 60)
    
    results = []
    failed_providers = []
    start_time = time.time()
    
    for i, provider in enumerate(providers[start_index:], start=start_index):
        provider_start = time.time()
        
        logger.info(f"\n[{i+1}/{len(providers)}] Processing: {provider['name']}")
        
        if provider.get('institution'):
            logger.info(f"  Institution: {provider['institution']}")
        if provider.get('specialty'):
            logger.info(f"  Specialty: {provider['specialty']}")
        
        try:
            # Execute search
            result = scraper.scrape_provider(
                name=provider["name"],
                institution=provider.get("institution"),
                specialty=provider.get("specialty"),
                npi=provider.get("npi"),
                location=provider.get("location")
            )
            
            # Check for errors
            if "error" in result["metadata"]:
                logger.error(f"  ❌ Error: {result['metadata']['error']}")
                failed_providers.append({
                    "provider": provider,
                    "error": result["metadata"]["error"]
                })
                
                # Log metrics
                metrics_logger.log_request_metrics(
                    request_id=result["metadata"]["request_id"],
                    provider_name=provider["name"],
                    success=False,
                    processing_time=time.time() - provider_start,
                    citations_count=0,
                    tokens_used=0,
                    confidence=0,
                    source_types={},
                    error_type=result["metadata"]["error"][:50]
                )
            else:
                # Success
                results.append(result)
                completed_providers.append(provider["name"])
                
                # Show summary
                citations_count = len(result["citations"])
                confidence = result["metadata"].get("overall_confidence", 0)
                
                logger.info(f"  ✓ Success: {citations_count} citations, {confidence:.1%} confidence")
                
                # Log metrics
                source_types = {}
                for c in result["citations"]:
                    st = c.get("source_type", "other")
                    source_types[st] = source_types.get(st, 0) + 1
                
                metrics_logger.log_request_metrics(
                    request_id=result["metadata"]["request_id"],
                    provider_name=provider["name"],
                    success=True,
                    processing_time=time.time() - provider_start,
                    citations_count=citations_count,
                    tokens_used=int(result["metadata"].get("api_tokens_used", 0)),
                    confidence=confidence,
                    source_types=source_types
                )
            
        except Exception as e:
            logger.error(f"  ❌ Unexpected error: {e}")
            failed_providers.append({
                "provider": provider,
                "error": str(e)
            })
            
            # Log metrics
            metrics_logger.log_request_metrics(
                request_id=f"error_{i}",
                provider_name=provider["name"],
                success=False,
                processing_time=time.time() - provider_start,
                citations_count=0,
                tokens_used=0,
                confidence=0,
                source_types={},
                error_type="exception"
            )
        
        # Save checkpoint
        if (i + 1) % 5 == 0 or (i + 1) == len(providers):
            checkpoint = {
                "last_completed_index": i,
                "timestamp": datetime.now().isoformat(),
                "total_providers": len(providers),
                "completed_providers": completed_providers,
                "failed_count": len(failed_providers)
            }
            
            with open(checkpoint_path, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            
            logger.debug(f"Checkpoint saved at index {i}")
        
        # Rate limiting
        if (i + 1) < len(providers):
            delay = 60 / args.rate_limit
            logger.debug(f"Rate limiting: waiting {delay:.1f} seconds")
            time.sleep(delay)
        
        # Show progress every 10 providers
        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1 - start_index)
            remaining = (len(providers) - i - 1) * avg_time
            
            logger.info(f"\n--- Progress Update ---")
            logger.info(f"Completed: {i + 1}/{len(providers)}")
            logger.info(f"Success rate: {len(results)}/{i + 1 - start_index} ({len(results)/(i + 1 - start_index):.1%})")
            logger.info(f"Estimated time remaining: {remaining/60:.1f} minutes")
    
    # Final summary
    total_time = time.time() - start_time
    
    logger.info("\n" + "=" * 60)
    logger.info("BULK PROCESSING COMPLETE")
    logger.info("=" * 60)
    
    logger.info(f"\nProcessing Summary:")
    logger.info(f"  Total providers: {len(providers)}")
    logger.info(f"  Successful: {len(results)}")
    logger.info(f"  Failed: {len(failed_providers)}")
    logger.info(f"  Success rate: {len(results)/len(providers):.1%}")
    logger.info(f"  Total time: {total_time/60:.1f} minutes")
    logger.info(f"  Average time per provider: {total_time/len(providers):.1f} seconds")
    
    # Show metrics summary
    metrics_summary = metrics_logger.get_summary()
    logger.info(f"\nQuality Metrics:")
    logger.info(f"  Average confidence: {metrics_summary['avg_confidence']:.1%}")
    logger.info(f"  Average citations: {metrics_summary['avg_citations']:.1f}")
    logger.info(f"  Total tokens used: {metrics_summary['total_tokens_used']:,}")
    
    # Save summary report
    summary_path = scraper.output_dir / "bulk_summary.json"
    summary = {
        "processing_date": datetime.now().isoformat(),
        "input_file": str(input_path),
        "total_providers": len(providers),
        "successful": len(results),
        "failed": len(failed_providers),
        "success_rate": len(results) / len(providers) if providers else 0,
        "total_time_seconds": total_time,
        "average_time_seconds": total_time / len(providers) if providers else 0,
        "completed_providers": completed_providers,
        "failed_providers": failed_providers,
        "metrics": metrics_summary
    }
    
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nSummary report saved to: {summary_path}")
    logger.info(f"Individual profiles saved in: {scraper.output_dir / 'profiles'}")
    logger.info(f"Citations saved in: {scraper.output_dir / 'citations'}")
    
    # Show failed providers if any
    if failed_providers:
        logger.warning(f"\nFailed Providers ({len(failed_providers)}):")
        for fp in failed_providers[:5]:
            logger.warning(f"  - {fp['provider']['name']}: {fp['error'][:100]}")
        
        if len(failed_providers) > 5:
            logger.warning(f"  ... and {len(failed_providers) - 5} more")
        
        # Save failed providers list
        failed_path = scraper.output_dir / "failed_providers.json"
        with open(failed_path, 'w') as f:
            json.dump(failed_providers, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\nFailed providers list saved to: {failed_path}")
    
    logger.info("\n" + "=" * 60)
    logger.info("Bulk processing completed!")
    logger.info("=" * 60)
    
    return 0 if len(failed_providers) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())