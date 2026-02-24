import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta

from azure.core.exceptions import AzureError
from azure.storage.blob import BlockBlobService
from django.conf import settings
from django.db import transaction

from proco.core import utils as core_utilities
from proco.giga_meter import models as giga_meter_models

logger = logging.getLogger('gigamaps.' + __name__)


class ADLSBackupError(Exception):
    """Exception for ADLS backup operations"""
    pass


class ConnectivityPingBackupManager:
    """ConnectivityPingBackupManager for backing up connectivity ping data to ADLS"""

    def __init__(self):
        self.config = settings.AZURE_DELTALAKE_CONFIG.get('GIGA_METER_PING_BACKUP', {})
        self.storage_account_name = self.config.get('AZURE_STORAGE_ACCOUNT_NAME')
        self.sas_token = self.config.get('AZURE_SAS_TOKEN')
        self.container_name = self.config.get('AZURE_BLOB_CONTAINER_NAME')
        self.delta_lake_path = self.config.get('DELTA_LAKE_PATH')
        self.retention_days = self.config.get('DATA_RETENTION_DAYS')
        self.buffer_days = self.config.get('HARD_DELETE_GRACE_PERIOD_DAYS')

        self._validate_config()
        self._storage_client = None

    def _validate_config(self):
        """Validate Azure configuration"""
        required_fields = ['AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_SAS_TOKEN', 'AZURE_BLOB_CONTAINER_NAME']
        missing_fields = [field for field in required_fields if not self.config.get(field)]

        if missing_fields:
            raise ADLSBackupError(
                f"Missing required Azure configuration: {', '.join(missing_fields)}. "
                f"Please check AZURE_DELTALAKE_CONFIG settings."
            )

    @property
    def storage_client(self):
        """Lazy initialization using legacy BlockBlobService"""
        if self._storage_client is None:
            self._storage_client = BlockBlobService(
                account_name=self.storage_account_name,
                sas_token=self.sas_token
            )
        return self._storage_client

    def _retry_operation(self, operation, max_retries=3, delay=1.0):
        """Retry wrapper for Azure operations with exponential backoff"""
        for attempt in range(max_retries):
            try:
                return operation()
            except (AzureError, Exception) as e:
                if attempt == max_retries - 1:
                    raise ADLSBackupError(f"Operation failed after {max_retries} attempts: {str(e)}")

                wait_time = delay * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time}s...")
                time.sleep(wait_time)

    def _create_parquet_file(self, data_frame, backup_date):
        """Create parquet file from DataFrame"""
        if data_frame.empty:
            raise ADLSBackupError(f"No data to backup for date: {backup_date}")

        parquet_file = f"/tmp/connectivity_ping_checks_{backup_date}.parquet"

        try:
            data_frame.to_parquet(
                parquet_file,
                engine="pyarrow",
                compression="snappy",
                index=False
            )
            logger.info(f"Created parquet file: {parquet_file} with {len(data_frame)} records")
            return parquet_file
        except Exception as e:
            raise ADLSBackupError(f"Failed to create parquet file: {str(e)}")

    def _upload_to_adls(self, local_file_path, backup_date):
        """Upload parquet file to Blob Storage using BlockBlobService"""
        blob_path = (
            f"{self.delta_lake_path}/"
            f"connectivity_ping_checks_{backup_date}.parquet"
        )

        def upload_operation():
            service = self.storage_client
            service.create_blob_from_path(
                container_name=self.container_name,
                blob_name=blob_path,
                file_path=local_file_path
            )
            return blob_path

        return self._retry_operation(upload_operation)

    def _cleanup_local_file(self, file_path):
        """Safely remove local temporary file"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            logger.warning(f"Failed to cleanup local file {file_path}: {str(e)}")

    def backup_single_date(self, backup_date):
        """Backup connectivity ping data for a single date"""
        logger.info(f"Starting backup for date: {backup_date}")
        parquet_file = None

        try:
            # Get data for the specific date
            backup_qs = giga_meter_models.GigaMeter_ConnectivityPingChecks.objects.filter(
                created_at__date=backup_date,
                is_deleted=False
            )

            if not backup_qs.exists():
                logger.warning(f"No data found for date: {backup_date}")
                return True, None

            # Create DataFrame and parquet file
            df = pd.DataFrame(backup_qs.values())
            parquet_file = self._create_parquet_file(df, backup_date)

            # Upload to ADLS
            adls_path = self._upload_to_adls(parquet_file, backup_date)
            logger.info(f"Successfully uploaded to ADLS: {adls_path}")

            # Soft delete records in database (atomic operation)
            with transaction.atomic():
                records_updated = backup_qs.update(
                    is_deleted=True,
                    deleted_at=core_utilities.get_current_datetime_object(timestamp=datetime.now())
                )
                logger.info(f"Soft deleted {records_updated} records for date: {backup_date}")

            return True, None

        except Exception as e:
            error_msg = f"Failed to backup data for date {backup_date}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

        finally:
            if parquet_file:
                self._cleanup_local_file(parquet_file)

    def backup_date_range(self, date_list):
        """Backup connectivity ping data for multiple dates"""
        successful_dates = []
        error_messages = []

        logger.info(f"Starting backup for {len(date_list)} dates")

        for backup_date in date_list:
            success, error_msg = self.backup_single_date(backup_date)

            if success:
                successful_dates.append(backup_date)
            else:
                error_messages.append(error_msg)

        logger.info(f"Backup completed. Success: {len(successful_dates)}, Failures: {len(error_messages)}")
        return successful_dates, error_messages

    def hard_delete_old_records(self, reference_date):
        """Hard delete old soft-deleted records based on retention policy"""
        if reference_date is None:
            reference_date = datetime.now().date()

        hard_delete_before_date = reference_date - timedelta(
            days=(self.retention_days + self.buffer_days)
        )

        logger.info(f"Hard deleting records created before: {hard_delete_before_date}")

        try:
            with transaction.atomic():
                deleted_count, _ = giga_meter_models.GigaMeter_ConnectivityPingChecks.objects.filter(
                    created_at__date__lt=hard_delete_before_date,
                    is_deleted=True
                ).delete()

                logger.info(f"Hard deleted {deleted_count} records")
                return deleted_count

        except Exception as e:
            logger.error(f"Failed to hard delete records: {str(e)}")
            raise ADLSBackupError(f"Hard delete operation failed: {str(e)}")


def backup_connectivity_ping_data(date_list):
    """Function to backup connectivity ping data to ADLS"""
    try:
        backup_manager = ConnectivityPingBackupManager()
        return backup_manager.backup_date_range(date_list)
    except ADLSBackupError as e:
        logger.error(f"Backup manager initialization failed: {str(e)}")
        return [], [str(e)]


def hard_delete_old_ping_data(reference_date=None):
    """Function to hard delete old soft deleted records"""
    try:
        backup_manager = ConnectivityPingBackupManager()
        return backup_manager.hard_delete_old_records(reference_date)
    except ADLSBackupError as e:
        logger.error(f"Hard delete operation failed: {str(e)}")
        return 0
