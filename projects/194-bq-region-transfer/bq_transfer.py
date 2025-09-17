"""
BigQuery Dataset Region Transfer Script
Handles dataset transfers between regions using proper authentication
"""

import os
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BigQueryTransfer:
    """Handle BigQuery dataset transfers between regions"""

    def __init__(self,
                 source_dataset: str,
                 dest_dataset: str,
                 bucket: str,
                 source_location: str = "us-east4",
                 dest_location: str = "US"):
        """
        Initialize transfer client

        Args:
            source_dataset: Source dataset ID
            dest_dataset: Destination dataset ID
            bucket: GCS bucket for temporary storage
            source_location: Source dataset location
            dest_location: Destination dataset location
        """
        self.source_dataset = source_dataset
        self.dest_dataset = dest_dataset
        self.bucket = bucket
        self.source_location = source_location
        self.dest_location = dest_location
        self.progress_file = Path("migration_progress.json")

        # Initialize BigQuery client with proper authentication
        self.client = self._create_client()
        self.project_id = self.client.project

        logger.info(f"Initialized transfer from {source_dataset} to {dest_dataset}")
        logger.info(f"Project: {self.project_id}, Bucket: {bucket}")

    def _create_client(self) -> bigquery.Client:
        """Create authenticated BigQuery client using service account from environment"""
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')

        if not service_account_json:
            logger.error("GCP_SERVICE_ACCOUNT_KEY not found in environment")
            logger.info("Please ensure .env file exists with GCP_SERVICE_ACCOUNT_KEY")
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY environment variable not set")

        try:
            # Parse service account JSON
            service_account_info = json.loads(service_account_json)

            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/bigquery',
                       'https://www.googleapis.com/auth/cloud-platform']
            )

            # Create and return client
            client = bigquery.Client(
                credentials=credentials,
                project=service_account_info.get('project_id')
            )

            logger.info(f"Successfully authenticated with project: {service_account_info.get('project_id')}")
            return client

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse service account JSON: {e}")
            raise ValueError("Invalid JSON in GCP_SERVICE_ACCOUNT_KEY")
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
            dataset.description = f"Migrated from {self.source_dataset}"

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
                'partitioned': table.time_partitioning is not None,
                'clustered': table.clustering_fields is not None
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_id}: {e}")
            raise

    def export_table(self, table_id: str) -> str:
        """Export table to GCS"""
        source_table = f"{self.project_id}.{self.source_dataset}.{table_id}"
        gcs_path = f"gs://{self.bucket}/{self.source_dataset}/{table_id}/*.parquet"

        logger.info(f"Exporting {table_id} to {gcs_path}")

        try:
            # Configure export job
            job_config = bigquery.ExtractJobConfig()
            job_config.destination_format = bigquery.DestinationFormat.PARQUET

            # Run export job
            extract_job = self.client.extract_table(
                source_table,
                gcs_path,
                job_config=job_config,
                location=self.source_location
            )

            # Wait for job to complete
            extract_job.result()
            logger.info(f"Successfully exported {table_id}")
            return gcs_path

        except Exception as e:
            logger.error(f"Failed to export {table_id}: {e}")
            raise

    def import_table(self, table_id: str, gcs_path: str):
        """Import table from GCS to destination dataset"""
        dest_table = f"{self.project_id}.{self.dest_dataset}.{table_id}"

        logger.info(f"Importing {table_id} from {gcs_path}")

        try:
            # Get source table schema to preserve structure
            source_table_ref = f"{self.project_id}.{self.source_dataset}.{table_id}"
            source_table = self.client.get_table(source_table_ref)

            # Configure load job
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.PARQUET
            job_config.schema = source_table.schema
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

            # Preserve partitioning if exists
            if source_table.time_partitioning:
                job_config.time_partitioning = source_table.time_partitioning

            # Preserve clustering if exists
            if source_table.clustering_fields:
                job_config.clustering_fields = source_table.clustering_fields

            # Run import job
            load_job = self.client.load_table_from_uri(
                gcs_path,
                dest_table,
                job_config=job_config,
                location=self.dest_location
            )

            # Wait for job to complete
            load_job.result()
            logger.info(f"Successfully imported {table_id}")

        except Exception as e:
            logger.error(f"Failed to import {table_id}: {e}")
            raise

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

    def cleanup_gcs(self, table_id: str):
        """Clean up temporary files from GCS"""
        gcs_prefix = f"gs://{self.bucket}/{self.source_dataset}/{table_id}/"

        try:
            from google.cloud import storage
            storage_client = storage.Client(credentials=self.client._credentials)
            bucket = storage_client.bucket(self.bucket)

            # List and delete blobs
            blobs = bucket.list_blobs(prefix=f"{self.source_dataset}/{table_id}/")
            for blob in blobs:
                blob.delete()

            logger.info(f"Cleaned up GCS files for {table_id}")

        except Exception as e:
            logger.warning(f"Failed to cleanup GCS for {table_id}: {e}")

    def migrate_table(self, table_id: str, cleanup: bool = True) -> bool:
        """Migrate a single table"""
        try:
            # Export to GCS
            gcs_path = self.export_table(table_id)

            # Import to destination
            self.import_table(table_id, gcs_path)

            # Verify transfer
            success = self.verify_transfer(table_id)

            # Cleanup if requested and successful
            if cleanup and success:
                self.cleanup_gcs(table_id)

            return success

        except Exception as e:
            logger.error(f"Failed to migrate {table_id}: {e}")
            return False

    def migrate_all(self, cleanup: bool = True):
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
            success = self.migrate_table(table_id, cleanup=cleanup)

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
    # Configuration
    config = {
        'source_dataset': 'op_20250702',
        'dest_dataset': 'op_20250702_US',
        'bucket': 'conflixis-temp',
        'source_location': 'us-east4',
        'dest_location': 'US'
    }

    # Allow command line override
    if len(sys.argv) > 1:
        config['source_dataset'] = sys.argv[1]
    if len(sys.argv) > 2:
        config['dest_dataset'] = sys.argv[2]

    try:
        # Create transfer instance
        transfer = BigQueryTransfer(**config)

        # Run migration
        transfer.migrate_all(cleanup=True)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()