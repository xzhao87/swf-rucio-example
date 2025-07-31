"""
Dataset management module for Rucio workflows.

This module provides the DatasetManager class for creating, managing,
and closing Rucio datasets, similar to the dataset operations in PanDA's
dataservice modules.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    UnsupportedOperation
)

from .exceptions import DatasetError, RucioClientError
from .utils import RucioUtils, ValidationUtils, MetadataUtils


class DatasetManager:
    """
    Manages Rucio dataset operations.
    
    This class provides methods for creating, opening, closing, and managing
    Rucio datasets, following patterns from PanDA's RucioAPI class.
    """
    
    def __init__(self, rucio_client: Optional[RucioClient] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize DatasetManager.
        
        Args:
            rucio_client: Optional Rucio client instance
            logger: Optional logger instance
        """
        self.client = rucio_client if rucio_client else RucioClient()
        self.logger = logger if logger else logging.getLogger(__name__)
        self._created_datasets = set()  # Track created datasets
        
    def create_dataset(
        self,
        dataset_name: str,
        scope: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        lifetime_days: Optional[int] = None,
        open_dataset: bool = True
    ) -> Dict[str, str]:
        """
        Create a new dataset in Rucio.
        
        This method follows the pattern from PanDA's register_dataset method.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            metadata: Dataset metadata
            lifetime_days: Dataset lifetime in days
            open_dataset: Whether to create dataset in open state
            
        Returns:
            Dictionary containing dataset identifiers (duid, vuid, etc.)
            
        Raises:
            DatasetError: If dataset creation fails
            ValidationError: If input validation fails
        """
        try:
            # Validate inputs
            ValidationUtils.validate_dataset_name(dataset_name)
            
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
            else:
                ValidationUtils.validate_scope(scope)
                
            self.logger.info(f"Creating dataset: {scope}:{dataset_name}")
            
            # Prepare metadata
            if metadata is None:
                metadata = {}
                
            # Add standard metadata
            dataset_metadata = MetadataUtils.create_dataset_metadata(**metadata)
            
            # Create dataset
            try:
                self.client.add_dataset(scope=scope, name=dataset_name, meta=dataset_metadata)
                self.logger.info(f"Successfully created dataset: {scope}:{dataset_name}")
            except DataIdentifierAlreadyExists:
                self.logger.warning(f"Dataset already exists: {scope}:{dataset_name}")
                
            # Set lifetime if specified
            if lifetime_days is not None:
                lifetime_seconds = lifetime_days * 86400
                try:
                    self.client.set_metadata(
                        scope=scope, 
                        name=dataset_name, 
                        key="lifetime", 
                        value=lifetime_seconds
                    )
                    self.logger.info(f"Set lifetime to {lifetime_days} days for {scope}:{dataset_name}")
                except Exception as e:
                    self.logger.warning(f"Failed to set lifetime: {str(e)}")
                    
            # Set dataset status to open if requested
            if open_dataset:
                try:
                    # Check current status first
                    metadata = self.client.get_metadata(scope=scope, name=dataset_name)
                    if not metadata.get('is_open', False):
                        self.client.set_status(scope=scope, name=dataset_name, open=True)
                        self.logger.info(f"Set dataset to open: {scope}:{dataset_name}")
                    else:
                        self.logger.debug(f"Dataset already open: {scope}:{dataset_name}")
                except Exception as e:
                    self.logger.warning(f"Failed to set dataset to open: {str(e)}")
                    
            # Generate identifiers
            vuid = RucioUtils.generate_vuid(scope, dataset_name)
            duid = vuid  # For simplicity, use same as vuid
            
            # Track created dataset
            self._created_datasets.add(f"{scope}:{dataset_name}")
            
            return {
                "duid": duid,
                "version": 1,
                "vuid": vuid,
                "scope": scope,
                "name": dataset_name
            }
            
        except Exception as e:
            error_msg = f"Failed to create dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def close_dataset(self, dataset_name: str, scope: Optional[str] = None) -> bool:
        """
        Close a dataset in Rucio.
        
        Similar to the close_dataset method in PanDA's RucioAPI.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            
        Returns:
            True if successful
            
        Raises:
            DatasetError: If dataset closing fails
        """
        try:
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.info(f"Closing dataset: {scope}:{dataset_name}")
            
            try:
                self.client.set_status(scope=scope, name=dataset_name, open=False)
                self.logger.info(f"Successfully closed dataset: {scope}:{dataset_name}")
                return True
            except (UnsupportedOperation, DataIdentifierNotFound) as e:
                self.logger.warning(f"Could not close dataset {scope}:{dataset_name}: {str(e)}")
                return True  # Return True as in PanDA implementation
                
        except Exception as e:
            error_msg = f"Failed to close dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e

    def attach_files(
        self,
        dataset_name: str,
        files: List[Dict[str, str]],
        dataset_scope: Optional[str] = None,
        rse: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Attach existing files to a dataset.
        
        Args:
            dataset_name: Name of the dataset
            files: List of file dictionaries with 'scope' and 'name' keys
            dataset_scope: Dataset scope (will be extracted if not provided)
            rse: RSE name for validation (optional)
            
        Returns:
            Dictionary with attachment results
            
        Raises:
            DatasetError: If attachment fails
        """
        try:
            # Extract dataset scope if not provided
            if dataset_scope is None:
                dataset_scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.info(f"Attaching {len(files)} files to dataset: {dataset_scope}:{dataset_name}")
            
            # Validate dataset exists
            try:
                self.client.get_did(scope=dataset_scope, name=dataset_name)
            except DataIdentifierNotFound:
                raise DatasetError(f"Dataset not found: {dataset_scope}:{dataset_name}")
            
            # Prepare file DIDs for attachment
            file_dids = []
            for file_info in files:
                if isinstance(file_info, dict):
                    if 'scope' not in file_info or 'name' not in file_info:
                        raise DatasetError("Each file must have 'scope' and 'name' keys")
                    file_dids.append({
                        'scope': file_info['scope'],
                        'name': file_info['name']
                    })
                else:
                    raise DatasetError("Files must be dictionaries with 'scope' and 'name' keys")
            
            # Validate files exist if RSE is specified
            if rse:
                for file_did in file_dids:
                    try:
                        replicas = list(self.client.list_replicas([file_did], rse_expression=rse))
                        if not replicas or not any(rse in replica.get('rses', {}) for replica in replicas):
                            self.logger.warning(f"File {file_did['scope']}:{file_did['name']} not found on RSE {rse}")
                    except Exception as e:
                        self.logger.warning(f"Could not verify file {file_did['scope']}:{file_did['name']} on RSE {rse}: {e}")
            
            # Attach files to dataset
            try:
                self.client.attach_dids(
                    scope=dataset_scope,
                    name=dataset_name,
                    dids=file_dids
                )
                
                attached_count = len(file_dids)
                self.logger.info(f"Successfully attached {attached_count} files to dataset: {dataset_scope}:{dataset_name}")
                
                return {
                    'dataset': f"{dataset_scope}:{dataset_name}",
                    'files_attached': attached_count,
                    'files': file_dids,
                    'rse': rse
                }
                
            except Exception as e:
                error_msg = f"Failed to attach files to dataset {dataset_scope}:{dataset_name}: {str(e)}"
                self.logger.error(error_msg)
                raise DatasetError(error_msg) from e
                
        except DatasetError:
            raise
        except Exception as e:
            error_msg = f"Failed to attach files to dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def get_dataset_metadata(self, dataset_name: str, scope: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get metadata of a dataset.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            
        Returns:
            Dataset metadata dictionary or None if not found
            
        Raises:
            DatasetError: If operation fails
        """
        try:
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.debug(f"Getting metadata for dataset: {scope}:{dataset_name}")
            
            try:
                metadata = self.client.get_metadata(scope=scope, name=dataset_name)
                
                # Add state information like in PanDA
                if metadata.get("is_open", False) and metadata.get("did_type") != "CONTAINER":
                    metadata["state"] = "open"
                else:
                    metadata["state"] = "closed"
                    
                return metadata
                
            except DataIdentifierNotFound:
                self.logger.warning(f"Dataset not found: {scope}:{dataset_name}")
                return None
                
        except Exception as e:
            error_msg = f"Failed to get metadata for dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def list_dataset_files(
        self, 
        dataset_name: str, 
        scope: Optional[str] = None,
        long_format: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        List files in a dataset.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            long_format: Whether to include extended metadata
            
        Returns:
            Dictionary mapping LFNs to file attributes
            
        Raises:
            DatasetError: If operation fails
        """
        try:
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.debug(f"Listing files in dataset: {scope}:{dataset_name}")
            
            files_dict = {}
            
            for file_info in self.client.list_files(scope=scope, name=dataset_name, long=long_format):
                lfn = str(file_info["name"])
                
                # Create file attributes similar to PanDA format
                attrs = {
                    "lfn": lfn,
                    "scope": str(file_info["scope"]),
                    "fsize": file_info["bytes"],
                    "filesize": file_info["bytes"],
                    "chksum": "ad:" + str(file_info.get("adler32", "")),
                    "checksum": "ad:" + str(file_info.get("adler32", "")),
                    "events": str(file_info.get("events", "")),
                }
                
                # Format GUID like in PanDA
                if "guid" in file_info and file_info["guid"]:
                    guid = str(file_info["guid"])
                    formatted_guid = RucioUtils.format_guid(guid)
                    attrs["guid"] = formatted_guid
                    
                if long_format and "lumiblocknr" in file_info:
                    attrs["lumiblocknr"] = str(file_info["lumiblocknr"])
                    
                files_dict[lfn] = attrs
                
            self.logger.info(f"Found {len(files_dict)} files in dataset {scope}:{dataset_name}")
            return files_dict
            
        except Exception as e:
            error_msg = f"Failed to list files in dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def get_dataset_count(self, dataset_name: str, scope: Optional[str] = None) -> int:
        """
        Get the number of files in a dataset.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            
        Returns:
            Number of files in the dataset
            
        Raises:
            DatasetError: If operation fails
        """
        try:
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.debug(f"Counting files in dataset: {scope}:{dataset_name}")
            
            count = 0
            try:
                for _ in self.client.list_files(scope=scope, name=dataset_name):
                    count += 1
                return count
            except DataIdentifierNotFound:
                self.logger.warning(f"Dataset not found: {scope}:{dataset_name}")
                return 0
                
        except Exception as e:
            error_msg = f"Failed to count files in dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def delete_dataset(
        self, 
        dataset_name: str, 
        scope: Optional[str] = None,
        grace_period_hours: Optional[int] = None
    ) -> bool:
        """
        Delete a dataset by setting its lifetime.
        
        Args:
            dataset_name: Name of the dataset
            scope: Dataset scope (will be extracted if not provided)
            grace_period_hours: Grace period before deletion in hours
            
        Returns:
            True if successful
            
        Raises:
            DatasetError: If operation fails
        """
        try:
            # Extract scope if not provided
            if scope is None:
                scope, dataset_name = RucioUtils.extract_scope(dataset_name)
                
            self.logger.info(f"Deleting dataset: {scope}:{dataset_name}")
            
            try:
                if grace_period_hours is not None:
                    lifetime_value = grace_period_hours * 3600
                else:
                    lifetime_value = 0.0001  # Very short lifetime
                    
                self.client.set_metadata(
                    scope=scope, 
                    name=dataset_name, 
                    key="lifetime", 
                    value=lifetime_value
                )
                
                self.logger.info(f"Set deletion lifetime for dataset: {scope}:{dataset_name}")
                
                # Remove from tracking
                dataset_id = f"{scope}:{dataset_name}"
                if dataset_id in self._created_datasets:
                    self._created_datasets.remove(dataset_id)
                    
                return True
                
            except DataIdentifierNotFound:
                self.logger.warning(f"Dataset not found for deletion: {scope}:{dataset_name}")
                return True
                
        except Exception as e:
            error_msg = f"Failed to delete dataset {dataset_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatasetError(error_msg) from e
            
    def get_created_datasets(self) -> List[str]:
        """
        Get list of datasets created by this manager instance.
        
        Returns:
            List of dataset names (scope:name format)
        """
        return list(self._created_datasets)
        
    def cleanup_created_datasets(self, grace_period_hours: int = 1) -> int:
        """
        Clean up all datasets created by this manager instance.
        
        Args:
            grace_period_hours: Grace period before deletion in hours
            
        Returns:
            Number of datasets marked for deletion
        """
        deleted_count = 0
        for dataset_id in list(self._created_datasets):
            try:
                scope, name = dataset_id.split(":", 1)
                self.delete_dataset(name, scope, grace_period_hours)
                deleted_count += 1
            except Exception as e:
                self.logger.error(f"Failed to cleanup dataset {dataset_id}: {str(e)}")
                
        return deleted_count
