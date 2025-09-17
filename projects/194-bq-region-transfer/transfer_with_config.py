#!/usr/bin/env python3
"""
Enhanced BigQuery Transfer Script with Configuration Support
Supports both config.yaml and command-line arguments
"""

import os
import sys
import yaml
import argparse
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

from bq_transfer import BigQueryTransfer
import logging

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file"""
    config_file = Path(config_path)

    if not config_file.exists():
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {}

    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def setup_logging(config: Dict[str, Any]):
    """Setup logging based on configuration"""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_file = log_config.get('file', 'transfer.log')

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def main():
    """Main execution with configuration support"""
    parser = argparse.ArgumentParser(description='BigQuery Dataset Region Transfer')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    parser.add_argument('--source-dataset', help='Override source dataset')
    parser.add_argument('--dest-dataset', help='Override destination dataset')
    parser.add_argument('--bucket', help='Override GCS bucket')
    parser.add_argument('--source-location', help='Override source location')
    parser.add_argument('--dest-location', help='Override destination location')
    parser.add_argument('--no-cleanup', action='store_true', help='Keep GCS files after transfer')
    parser.add_argument('--dry-run', action='store_true', help='List tables but don\'t transfer')
    parser.add_argument('--table', help='Transfer only specific table')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Setup logging
    setup_logging(config)

    logger.info("=" * 60)
    logger.info("BigQuery Region Transfer - Enhanced Version")
    logger.info("=" * 60)

    # Get transfer configuration
    transfer_config = config.get('transfer', {})

    # Build transfer parameters (config -> command line override)
    params = {
        'source_dataset': args.source_dataset or
                         transfer_config.get('source', {}).get('dataset', 'op_20250702'),
        'dest_dataset': args.dest_dataset or
                       transfer_config.get('destination', {}).get('dataset', 'op_20250702_US'),
        'bucket': args.bucket or
                 transfer_config.get('gcs', {}).get('bucket', 'conflixis-temp'),
        'source_location': args.source_location or
                          transfer_config.get('source', {}).get('location', 'us-east4'),
        'dest_location': args.dest_location or
                        transfer_config.get('destination', {}).get('location', 'US')
    }

    cleanup = transfer_config.get('gcs', {}).get('cleanup_after_transfer', True) and not args.no_cleanup

    logger.info(f"Configuration:")
    logger.info(f"  Source: {params['source_dataset']} ({params['source_location']})")
    logger.info(f"  Destination: {params['dest_dataset']} ({params['dest_location']})")
    logger.info(f"  GCS Bucket: {params['bucket']}")
    logger.info(f"  Cleanup after transfer: {cleanup}")

    if args.dry_run:
        logger.info("\n🔍 DRY RUN MODE - No actual transfers will occur")

    try:
        # Create transfer instance
        transfer = BigQueryTransfer(**params)

        if args.dry_run:
            # Just list tables
            logger.info("\nTables to transfer:")
            tables = transfer.list_tables()

            total_size = 0
            for i, table_id in enumerate(tables, 1):
                try:
                    info = transfer.get_table_info(table_id)
                    size_mb = info['num_bytes'] / 1024 / 1024
                    total_size += info['num_bytes']
                    logger.info(f"  {i:3}. {table_id:40} - {info['num_rows']:12,} rows, {size_mb:10,.2f} MB")
                except:
                    logger.info(f"  {i:3}. {table_id}")

            logger.info(f"\nTotal: {len(tables)} tables, {total_size / 1024 / 1024 / 1024:.2f} GB")

        elif args.table:
            # Transfer single table
            logger.info(f"\nTransferring single table: {args.table}")
            success = transfer.migrate_table(args.table, cleanup=cleanup)

            if success:
                logger.info(f"✅ Successfully transferred {args.table}")
            else:
                logger.error(f"❌ Failed to transfer {args.table}")
                sys.exit(1)

        else:
            # Transfer all tables
            transfer.migrate_all(cleanup=cleanup)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Transfer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        sys.exit(1)

    logger.info("\n✨ Transfer process completed")


if __name__ == "__main__":
    main()