#!/usr/bin/env python3
"""
BigQuery Direct Dataset Copy - No GCS Required
Uses BigQuery's native copy_table API for cross-region transfers
"""

import os
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('direct_transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BigQueryDirectTransfer:
    """Direct BigQuery dataset transfer without GCS"""

    def __init__(self,
                 source_dataset: str,
                 dest_dataset: str,
                 dest_location: str = "US"):
        """
        Initialize direct transfer client

        Args:
            source_dataset: Source dataset ID
            dest_dataset: Destination dataset ID
            dest_location: Destination dataset location
        """
        self.source_dataset = source_dataset
        self.dest_dataset = dest_dataset
        self.dest_location = dest_location
        self.progress_file = Path("direct_migration_progress.json")

        # Initialize BigQuery client
        self.client = self._create_client()
        self.project_id = self.client.project

        logger.info(f"Initialized direct transfer from {source_dataset} to {dest_dataset}")
        logger.info(f"Project: {self.project_id}, Destination location: {dest_location}")

    def _create_client(self) -> bigquery.Client:
        """Create authenticated BigQuery client"""
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')

        if not service_account_json:
            logger.error("GCP_SERVICE_ACCOUNT_KEY not found in environment")
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable not set")

        try:
            service_account_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )

            client = bigquery.Client(
                credentials=credentials,
                project=service_account_info.get('project_id')
            )

            logger.info(f"Successfully authenticated with project: {service_account_info.get('project_id')}")
            return client

        except Exception as e:
            logger.error(f"Failed to create BigQuery client: {e}")
            raise

    def _load_progress(self) -> Dict[str, List[str]]:
        """Load migration progress from file"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {"completed": [], "failed": []}

    def _save_progress(self, progress: Dict[str, List[str]]):
        """Save migration progress to file"""
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)

    def ensure_destination_dataset(self):
        """Create destination dataset if it doesn't exist"""
        dataset_id = f"{self.project_id}.{self.dest_dataset}"

        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Destination dataset {dataset_id} already exists")
        except Exception:
            # Create dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.dest_location
            dataset.description = f"Migrated from {self.source_dataset} (direct copy)"

            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created destination dataset {dataset_id} in {self.dest_location}")

    def list_tables(self) -> List[str]:
        """List all tables in source dataset"""
        dataset_id = f"{self.project_id}.{self.source_dataset}"

        try:
            tables = list(self.client.list_tables(dataset_id))
            table_ids = [table.table_id for table in tables]
            logger.info(f"Found {len(table_ids)} tables in {dataset_id}")
            return table_ids
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise

    def get_table_info(self, table_id: str) -> Dict[str, Any]:
        """Get information about a table"""
        table_ref = f"{self.project_id}.{self.source_dataset}.{table_id}"

        try:
            table = self.client.get_table(table_ref)
            return {
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': table.schema,
                'time_partitioning': table.time_partitioning,
                'clustering_fields': table.clustering_fields
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_id}: {e}")
            raise

    def copy_table_direct(self, table_id: str) -> bool:
        """Copy table directly using BigQuery's copy_table API"""
        source_table_id = f"{self.project_id}.{self.source_dataset}.{table_id}"
        dest_table_id = f"{self.project_id}.{self.dest_dataset}.{table_id}"

        logger.info(f"Copying {table_id} directly in BigQuery...")

        try:
            # Configure copy job
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

            # Get source table to preserve schema
            source_table = self.client.get_table(source_table_id)

            # Create destination table with same schema if needed
            try:
                dest_table = self.client.get_table(dest_table_id)
                logger.info(f"Destination table {table_id} exists, will overwrite")
            except:
                # Table doesn't exist, will be created by copy job
                logger.info(f"Destination table {table_id} will be created")

            # Run copy job
            job = self.client.copy_table(
                source_table_id,
                dest_table_id,
                job_config=job_config
            )

            # Wait for job to complete
            result = job.result()

            logger.info(f"✓ Successfully copied {table_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to copy {table_id}: {e}")

            # Try alternative: Create table from query
            try:
                logger.info(f"Attempting alternative method using CREATE TABLE AS SELECT...")

                query = f"""
                CREATE OR REPLACE TABLE `{dest_table_id}`
                PARTITION BY DATE(created_date)  -- Adjust based on actual partitioning
                CLUSTER BY cluster_field  -- Adjust based on actual clustering
                OPTIONS(
                    description="Migrated from {source_table_id}"
                )
                AS SELECT * FROM `{source_table_id}`
                """

                # For tables without partitioning/clustering
                simple_query = f"""
                CREATE OR REPLACE TABLE `{dest_table_id}`
                OPTIONS(
                    description="Migrated from {source_table_id}"
                )
                AS SELECT * FROM `{source_table_id}`
                """

                job_config = bigquery.QueryJobConfig()
                query_job = self.client.query(simple_query, job_config=job_config)
                query_job.result()

                logger.info(f"✓ Successfully copied {table_id} using CREATE TABLE AS SELECT")
                return True

            except Exception as e2:
                logger.error(f"Alternative method also failed: {e2}")
                return False

    def verify_transfer(self, table_id: str) -> bool:
        """Verify table was transferred correctly"""
        source_table = f"{self.project_id}.{self.source_dataset}.{table_id}"
        dest_table = f"{self.project_id}.{self.dest_dataset}.{table_id}"

        try:
            # Get row counts
            source_count_query = f"SELECT COUNT(*) as count FROM `{source_table}`"
            dest_count_query = f"SELECT COUNT(*) as count FROM `{dest_table}`"

            source_result = list(self.client.query(source_count_query).result())[0]
            dest_result = list(self.client.query(dest_count_query).result())[0]

            source_rows = source_result.count
            dest_rows = dest_result.count

            if source_rows == dest_rows:
                logger.info(f"✓ Verification passed for {table_id}: {source_rows:,} rows")
                return True
            else:
                logger.error(f"✗ Row count mismatch for {table_id}: source={source_rows:,}, dest={dest_rows:,}")
                return False

        except Exception as e:
            logger.error(f"Failed to verify {table_id}: {e}")
            return False

    def migrate_table(self, table_id: str) -> bool:
        """Migrate a single table using direct copy"""
        try:
            # Copy table
            success = self.copy_table_direct(table_id)

            # Verify if successful
            if success:
                return self.verify_transfer(table_id)

            return False

        except Exception as e:
            logger.error(f"Failed to migrate {table_id}: {e}")
            return False

    def migrate_all(self):
        """Migrate all tables with progress tracking"""
        # Ensure destination dataset exists
        self.ensure_destination_dataset()

        # Get list of tables
        tables = self.list_tables()

        # Load progress
        progress = self._load_progress()
        completed = set(progress.get("completed", []))
        failed = set(progress.get("failed", []))

        logger.info(f"Found {len(tables)} tables total")
        logger.info(f"Already completed: {len(completed)}, Failed: {len(failed)}")

        # Process each table
        for i, table_id in enumerate(tables, 1):
            if table_id in completed:
                logger.info(f"[{i}/{len(tables)}] Skipping {table_id} (already completed)")
                continue

            logger.info(f"[{i}/{len(tables)}] Migrating {table_id}...")

            # Get table info
            try:
                info = self.get_table_info(table_id)
                logger.info(f"  - Rows: {info['num_rows']:,}, Size: {info['num_bytes'] / 1024 / 1024:.2f} MB")
            except:
                pass

            # Migrate table
            success = self.migrate_table(table_id)

            if success:
                completed.add(table_id)
                if table_id in failed:
                    failed.remove(table_id)
                logger.info(f"✅ {table_id} migrated successfully")
            else:
                failed.add(table_id)
                logger.error(f"❌ {table_id} migration failed")

            # Save progress
            progress = {
                "completed": list(completed),
                "failed": list(failed),
                "last_updated": datetime.now().isoformat()
            }
            self._save_progress(progress)

        # Final summary
        logger.info("=" * 50)
        logger.info("Migration Summary:")
        logger.info(f"✅ Completed: {len(completed)}/{len(tables)}")
        if failed:
            logger.warning(f"❌ Failed: {len(failed)} tables")
            logger.warning(f"Failed tables: {', '.join(failed)}")
        else:
            logger.info("🎉 All tables migrated successfully!")


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='BigQuery Direct Dataset Transfer (No GCS Required)')
    parser.add_argument('--source-dataset', default='op_20250702', help='Source dataset ID')
    parser.add_argument('--dest-dataset', default='op_20250702_US', help='Destination dataset ID')
    parser.add_argument('--dest-location', default='US', help='Destination location')
    parser.add_argument('--table', help='Transfer only specific table')

    args = parser.parse_args()

    try:
        # Create transfer instance
        transfer = BigQueryDirectTransfer(
            source_dataset=args.source_dataset,
            dest_dataset=args.dest_dataset,
            dest_location=args.dest_location
        )

        if args.table:
            # Transfer single table
            logger.info(f"Transferring single table: {args.table}")
            success = transfer.migrate_table(args.table)
            if not success:
                sys.exit(1)
        else:
            # Transfer all tables
            transfer.migrate_all()

    except KeyboardInterrupt:
        logger.info("\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()